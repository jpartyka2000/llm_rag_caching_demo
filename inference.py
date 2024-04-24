import sys
import os
import time
import json
import requests
import nltk
import wordninja
import openai
import re
import mysql.connector

from fastapi import FastAPI, Request, Query
from GPTUtils import GPTUtils
from CreateFinalPrompt import CreateFinalPrompt
from AnonymizePrompt import AnonymizePrompt
from QueryCache import QueryCache
from typing import List, Optional
from pydantic import BaseModel

app = FastAPI()

openai.api_key = os.environ['OPENAI_API_KEY']

mysql_host = os.environ['MYSQL_COPILOT_HOST']
mysql_username = os.environ['MYSQL_COPILOT_USERNAME']
mysql_password = os.environ['MYSQL_COPILOT_PASSWORD']

#background context str to be injected into every prompt
background_context_str = ""

#sqlite3 db connection and cursor
conn = None
cur = None

#query-oriented data structures that come from SQLite DB
query_to_threshold_dict = {}
query_to_signature_dict_dict = {}
query_to_sql_dict = {}

try:

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_username,
        password=mysql_password,
        database="copilot"
    )

except Exception:
    print("connection to DB failed, skipping...")


class DataRequest(BaseModel):
    user_question_str: str
    period_id_list: Optional[List[int]]
    participant_id: int
    position_id_list: Optional[List[int]]


class AnswerConfirmation(BaseModel):
    user_question_str: str
    answer_sql_str: str
    entitized_question_str: str
    cache_or_llm: str
    is_answer_correct: bool

#this endpoint will allow a user to add a new query to the cache. This is something like RLHF
@app.get("/confirm_answer_correctness")
async def confirm_answer_correctness(request: AnswerConfirmation):
    
    global conn
    
    start_time = time.time()
    
    cur = conn.cursor()
    
    user_question_str = request.user_question_str
    answer_sql_str = request.answer_sql_str
    entitized_question_str = request.entitized_question_str
    cache_or_llm = request.cache_or_llm
    is_answer_correct = request.is_answer_correct
    
    query_mappings_tuple_list = []

    #get all data from query_mappings table to prepare for either an upvote or downvote from user about their answer
    try:
        
        # Select and display all rows from the table
        cur.execute("SELECT * FROM query_feedback;")
        query_feedback_tuple_list = cur.fetchall()
            
    except Exception:
        print("Unable to read data from query_feedback table. Exiting confirm_answer_correctness.")
        total_time_seconds = time.time() - start_time
        return {"result":"failure", "total_time":total_time_seconds }
    
    #if answer came from the cache, then we just need to match entitized_question_str with the corresponding row from query_mappings_tuple_list
    #and increment either upvote or downvote depending on value of 'is_answer_correct'
    
    if cache_or_llm == "cache":
        
        for this_query_feedback in query_feedback_tuple_list:
            
            qf_id = int(this_query_feedback[0])
            qf_user_question_str = this_query_feedback[1]
            qf_answer_sql_str = this_query_feedback[2]
            qf_upvote = int(this_query_feedback[3])
            qf_downvote = int(this_query_feedback[4])
            qf_entitized_query = this_query_feedback[5]
            
            if qf_user_question_str == user_question_str:
                
                new_vote_count = 0
                
                #determine if user upvoted or downvoted their answer
                if is_answer_correct == True:
                    
                    vote_column = "upvotes"
                    new_vote_count = qf_upvote + 1
                                        
                else:
                    
                    vote_column = "downvotes"
                    new_vote_count = qf_downvote + 1
                
                try:
                    
                    update_query_str = f"UPDATE query_feedback SET %s = %s WHERE id = %s;" % (vote_column, str(new_vote_count), str(qf_id))

                    #we found the query, now we just need to upvote it
                    cur.execute(update_query_str)

                    #Commit the changes
                    conn.commit()
                
                except Exception:
                    print("Error updating query_feedback table, returning failure...")
                
                #terminate for loop here for the else statement below to work
                break
            
        else:
            
            #if we get here, that means that we have to insert a new query into the query_feedback table
            #we will insert the row whether the answer is correct or not, because collecting stats on unpopular queries is also 
            #useful
            
            latest_id_value = -1
            
            try: 
                #get latest_id value from query_mappings
                latest_id_select_query = f"SELECT id FROM query_feedback ORDER BY id DESC LIMIT 1;"

                cur.execute(latest_id_select_query)
                
                for this_row in cur.fetchall():
                    latest_id_value = int(this_row[0]) + 1
            
            except Exception:
                print("Error retrieving latest id from query_feedback table, returning failure...")
                
                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
            
            #next, insert new row of data into query_feedback table
            
            new_row_data = (latest_id_value, user_question_str, answer_sql_str, 0, 0, entitized_question_str)
            
            insert_query_str = f"INSERT INTO query_feedback (id, query, sql_result, upvotes, downvotes, entitized_query) VALUES (%s, %s, %s, %s, %s, %s)"
            
            #insert into query_feedback table
            try:

                cur.execute(insert_query_str, new_row_data)

                #Commit the changes
                conn.commit()

            except Exception:
                print("Error inserting new query row into query_feedback table, returning failure...")

                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
    
                    
    elif cache_or_llm == "llm":
        
        #for llm, we know that the query returned was not in the cache. We want to be able to add it to the cache
        #but only if the user upvoted it. The query will only be used from the cache if the number of upvotes exceeds the number of downvotes by 5
        #this threshold will change over time
        
        if is_answer_correct == True:
            
            latest_id_value = -1
            
            try: 
                #get latest_id value from query_mappings
                latest_id_select_query = f"SELECT id FROM query_mappings ORDER BY id DESC LIMIT 1;"

                cur.execute(latest_id_select_query)
                
                for this_row in cur.fetchall():
                    latest_id_value = int(this_row[0]) + 1
            
            except Exception:
                print("Error retrieving latest id from query_mappings table, returning failure...")
                
                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
            
            #next, I need to create the signature_dict for the new query
            #for now, I will just count the number of entities
            
            #person entities
            person_entity_count = entitized_question_str.count("person_entity_")
            org_entity_count = entitized_question_str.count("org_entity_")
            date_entity_count = entitized_question_str.count("date_entity_")
            
            signature_dict = {"date_count":date_entity_count, "org_count":org_entity_count, "person_count":person_entity_count, "or":[], "and":[]}
            
            new_row_data = (latest_id_value, entitized_question_str, answer_sql_str, .85, str(signature_dict))
            
            insert_query_str = f"INSERT INTO query_mappings (id, query, sql_result, threshold, signature_dict) VALUES (%s, %s, %s, %s, %s)"
            
            #insert into query_mappings table
            try:

                cur.execute(insert_query_str, new_row_data)

                #Commit the changes
                conn.commit()

            except Exception:
                print("Error inserting new query row into query_mappings table, returning failure and skipping...")

                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
            
            #also insert into query_feedback table
            #get latest id from query_feedback table
            
            latest_id_feedback_value = -1
            
            try: 
                #get latest_id value from query_mappings
                latest_id_feedback_select_query = f"SELECT id FROM query_feedback ORDER BY id DESC LIMIT 1;"

                cur.execute(latest_id_feedback_select_query)
                
                for this_row in cur.fetchall():
                    latest_id_feedback_value = int(this_row[0]) + 1
            
            except Exception:
                print("Error retrieving latest id from query_feedback table, returning failure...")
                
                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
            
            #insert new row into query_feedback table
            
            new_row_data_feedback = (latest_id_feedback_value, user_question_str, answer_sql_str, 1, 0, entitized_question_str)
            
            insert_query_str_feedback = f"INSERT INTO query_feedback (id, query, sql_result, upvotes, downvotes, entitized_query) VALUES (%s, %s, %s, %s, %s, %s)"
            
            try:

                cur.execute(insert_query_str_feedback, new_row_data_feedback)

                #Commit the changes
                conn.commit()

            except Exception:
                print("Error inserting new query row into query_feedback table, returning failure...")

                total_time_seconds = time.time() - start_time
                return {"result":"failure", "total_time":total_time_seconds }
            
             
    total_time_seconds = time.time() - start_time
    
    return {"result":"success", "total_time":total_time_seconds }
    
    

def init():
    
    global background_context_str
    global conn
    global query_to_threshold_dict
    global query_to_signature_dict_dict
    global query_to_sql_dict
    
    background_context_str = ""
    
    #add schema details
    schema_details_str = ""
    
    with open("metadata_anon_xactlyapps_commission.sql" ,"r") as readfile:
        schema_details_str = readfile.read() + '\n'
        
    #also add additional prompt instructions
    additional_prompt_instructions_str = ""
    
    with open("prompt_xactlyapps.md" ,"r") as readfile2:
        additional_prompt_instructions_str = readfile2.read() + '\n'
    
    #create cursor from existing connection
    cur = conn.cursor()
    
     #get DB rows from query_mappings table
    try:
        
        # Select and display all rows from the table
        cur.execute("SELECT * FROM query_mappings;")
        for row in cur.fetchall():

            #populate query-related data structures
            this_query = row[1]
            this_sql = row[2]
            this_threshold = row[3]
            this_signature_dict = json.loads(row[4])

            query_to_threshold_dict[this_query] = this_threshold
            query_to_sql_dict[this_query] = this_sql
            query_to_signature_dict_dict[this_query] = this_signature_dict
        
    except Exception:
        print("Unable to read data from query_mappings table. Skipping...")
   
    background_context_str = schema_details_str + additional_prompt_instructions_str
    
    return background_context_str



@app.get("/answer_prompt")
async def answer_prompt(request: DataRequest):
    
    global background_context_str
    global query_to_threshold_dict
    global query_to_signature_dict_dict
    global query_to_sql_dict
    
    user_question_str = request.user_question_str
    period_id_list = request.period_id_list
    participant_id = request.participant_id
    position_id_list = request.position_id_list
    
    background_context_str = init()
    
    start_time = time.time()
    
    #make sure that query has a punctuation mark at the end of it. Otherwise the service breaks
    if user_question_str[-1] not in ['.','?','!']:
        print("no punctuation at end of prompt...fixing...")
        user_question_str = user_question_str + '?' 
    
    print("QueryCache about to be called...")
    
    #first determine if the query is irrelevant OR already matches one of our supported queries. In both cases, there is no need to send the query to an LLM.
    query_cache_obj = QueryCache(user_question_str, period_id_list, participant_id, position_id_list, openai.api_key, query_to_threshold_dict, query_to_signature_dict_dict, query_to_sql_dict)
    
    semantic_matching_query, best_matching_entitized_query = query_cache_obj.get_semantic_matching_query()
    
    #if we get an error message, then the query is either not relevant or cannot be processed by the current version of our LLM service
    if semantic_matching_query == "This query cannot be answered by our existing LLM functionality.":
        total_time_seconds = time.time() - start_time
        return {"answer":semantic_matching_query, "entitized_query":"", "total_time":total_time_seconds}
    
    #if we get an actual query, then return that query
    if semantic_matching_query != "":
        
        #for formatting purposes, let's reduce the amount of whitespace between each pair of words to 1
        semantic_matching_query = re.sub(r'\s+', ' ', semantic_matching_query)
        
        print("Returning a cached query....")
        total_time_seconds = time.time() - start_time
        return {"answer":semantic_matching_query, "entitized_query":best_matching_entitized_query, "cache_or_llm":"cache", "total_time":total_time_seconds}
    
    #if we get here, then we will be sending our query to the LLM for processing
    print("Sending prompt to LLM...")
    
    #first, get curr_entitized_prompt
    curr_entitized_prompt = query_cache_obj.get_current_prompt()
    
    user_question_str = query_cache_obj.get_current_prompt()
    
    this_anonymize_prompt_obj = AnonymizePrompt(user_question_str, participant_id, period_id_list, position_id_list)
    anon_question_str = this_anonymize_prompt_obj.anonymize_prompt()
    
    #append background context to user_question_str
    anon_question_str = background_context_str + '\n' + anon_question_str
        
    print("Final anonynimized prompt is: ")
    with open("anon_prompt.txt", 'w') as writefile:
        writefile.write(anon_question_str)
        
    
    #instantiate GPTUtils object and call one of the GPT models
    gpt_obj = GPTUtils(openai.api_key, background_context_str)
    this_anon_answer_str = gpt_obj.call_gpt(anon_question_str)
        
    #remove newlines added by GPT
    this_anon_answer_str = this_anon_answer_str.replace("\n"," ")
    
    #deanonymize this_anon_answer_str
    final_answer_str = this_anonymize_prompt_obj.deanonymize_answer(this_anon_answer_str)
    
    #remove extra whitespace in query
    final_answer_str = re.sub(r'\s+', ' ', final_answer_str)
    
    #lastly, check if participant_name has been selected. If not, then insert it as the first column selected
    select_participant_name_pattern_str = r'participant_name'
    
    if not bool(re.search(select_participant_name_pattern_str, final_answer_str, re.IGNORECASE)):
        
        select_pattern_str = r'^Select|^SELECT|^select'
        final_answer_str = re.sub(select_pattern_str, "Select participant_name,", final_answer_str, re.IGNORECASE)
    
    total_time_seconds = time.time() - start_time
    
    return {"answer":final_answer_str, "orig_query":user_question_str, "entitized_query":curr_entitized_prompt, "cache_or_llm": "llm", "total_time":total_time_seconds }


#initialize GPT with opening background prompt
#init()
    
    
    
    
    
    
    

