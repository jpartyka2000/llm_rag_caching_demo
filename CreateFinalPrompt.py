import os
import sys
import warnings
import torch
import json
import re

from sentence_transformers import SentenceTransformer, util
from nltk.tokenize import word_tokenize

prompt_file = "prompt_xactlyapps.md"
sql_file = "metadata_anon_xactlyapps.sql"

class CreateFinalPrompt:
    
    def __init__(self, user_question_str):
        
        #prompt variables
        self.sample_prompt_str = user_question_str
        self.sample_prompt_embedding = None
        self.prompt_template_str = ""
        self.final_prompt_str = ""
        
        #embedding variables
        self.embedding_model_name = "all-MiniLM-L6-v2"
        self.embedding_function = SentenceTransformer(self.embedding_model_name)
        self.sql_table_schema_desc_to_embedding_value_dict = {}
        self.sql_table_schema_desc_to_cosine_similarity_dict = {}
        
        #SQL context generation variables
        self.sql_table_schema_desc_list = []
        self.sql_table_schema_desc_str = ""
        self.sql_table_name_to_table_desc_str_dict = {}
        self.sql_table_name_to_cos_sim_dict = {}
        self.candidate_sql_table_list = []
        self.final_join_table_description_list = []
        self.final_join_table_description_str = ""
        self.sql_table_context_str = ""
        self.sql_table_name_list = []
        
        #file string variables
        self.sql_table_schema_desc_str = ""
        self.sql_table_schema_desc_list = []
        self.join_table_description_str = ""
        self.join_table_description_list = []
        
        #function calls
        self.initialize_sql_data_structures()
        self.read_prompt_files()
        self.populate_sql_data_structures()
        self.filter_sql_tables()
        #self.filter_join_possibilities()
        self.build_final_prompt() 
        
    
    def initialize_sql_data_structures(self):
        
        self.sql_table_name_to_table_desc_str_dict['xc_user'] = ""
        self.sql_table_name_to_table_desc_str_dict['xc_participant'] = ""
        self.sql_table_name_to_table_desc_str_dict['xc_commission'] = ""
        self.sql_table_name_to_table_desc_str_dict['xc_period'] = ""
        self.sql_table_name_to_table_desc_str_dict['xc_pos_hierarchy'] = ""
        self.sql_table_name_to_table_desc_str_dict['xc_pos_part_assignment'] = ""
        
        self.sql_table_name_to_cos_sim_dict['xc_user'] = -1.0
        self.sql_table_name_to_cos_sim_dict['xc_participant'] = -1.0
        self.sql_table_name_to_cos_sim_dict['xc_commission'] = -1.0
        self.sql_table_name_to_cos_sim_dict['xc_period'] = -1.0
        self.sql_table_name_to_cos_sim_dict['xc_pos_hierarchy'] = -1.0
        self.sql_table_name_to_cos_sim_dict['xc_pos_part_assignment'] = -1.0
        
        self.sql_table_name_list = list(self.sql_table_name_to_table_desc_str_dict.keys())
        

    def read_prompt_files(self):
        
        global prompt_file
        global sql_file
        
        #the sql table schema context str
        with open(sql_file, "r") as readfile:
            self.sql_table_schema_desc_str = readfile.read()
       
        #the prompt template str
        with open(prompt_file, "r") as readfile2:
            self.final_prompt_str = readfile2.read()
    
    
    def populate_sql_data_structures(self):
        
        self.sql_table_schema_desc_list = self.sql_table_schema_desc_str.split("\n\n")

        #the last element of sql_table_schema_desc_list will become join_table_description_str
        self.join_table_description_str = self.sql_table_schema_desc_list[-1]

        #also split join_table_description_str
        self.join_table_description_list = self.join_table_description_str.split("\n")

        #remove any empty elements from sql_table_schema_desc_list
        self.sql_table_schema_desc_list = list(filter(None, self.sql_table_schema_desc_list))

        #remove -1 element from sql_table_schema_desc_list
        del self.sql_table_schema_desc_list[-1]

        #create embeddings for each desc
        self.sql_table_schema_desc_to_embedding_value_dict = {}

        #we will be calculating the cosine similarity of the sample prompt against each table description embedding
        self.sql_table_schema_desc_to_cosine_similarity_dict = {}

        for schemaidx, this_table_schema_desc_str in enumerate(self.sql_table_schema_desc_list):
            self.sql_table_schema_desc_to_embedding_value_dict[this_table_schema_desc_str] = self.embedding_function.encode(this_table_schema_desc_str, convert_to_tensor=True)

            #initialize cos sim values
            self.sql_table_schema_desc_to_cosine_similarity_dict[this_table_schema_desc_str] = -99.0

            #populate sql_table_name_to_table_desc_str_dict
            this_sql_table_name = self.sql_table_name_list[schemaidx]

            self.sql_table_name_to_table_desc_str_dict[this_sql_table_name] = this_table_schema_desc_str


        #remove any empty elements from join_table_description_list
        #self.join_table_description_list = list(filter(None, self.join_table_description_list))

        #also remove any leading SQL comments from elements of join_table_description_list
        #self.join_table_description_list = [this_join_desc_str.replace("-- ","") for this_join_desc_str in self.join_table_description_list]
        
        #create embedding for sample_prompt_str
        self.sample_prompt_embedding = self.embedding_function.encode(self.sample_prompt_str, convert_to_tensor=True)


    def filter_sql_tables(self):
        
        #print("For the prompt: " + self.sample_prompt_str + "......")

        #for this_sql_table_name in self.sql_table_name_list:

        #    this_table_schema_str = self.sql_table_name_to_table_desc_str_dict[this_sql_table_name]
        #    this_table_schema_embedding_value = self.sql_table_schema_desc_to_embedding_value_dict[this_table_schema_str]

        #    self.sql_table_name_to_cos_sim_dict[this_sql_table_name] = round(util.cos_sim(self.sample_prompt_embedding, this_table_schema_embedding_value)[0][0].item(), 5)

        #    print("The cos sim for the table " + this_sql_table_name + " is: " + str(self.sql_table_name_to_cos_sim_dict[this_sql_table_name]))
        #    print('----------')

        #filter down sql_table_name_to_table_desc_str_dict based on cos sim values, exact table name matches and overlapping table membership
        #any table with a matchng cos sim value >= .10 will be included, as well as table names where the exact match of the table can be found in the prompt

        #for this_sql_table_name, this_cos_sim_value in self.sql_table_name_to_cos_sim_dict.items():

        #    if this_cos_sim_value >= 0.10 or this_sql_table_name == 'xc_commission':
        #        self.candidate_sql_table_list.append(this_sql_table_name)
        #        continue

        #also check for an exact match
        #this_sql_table_desc_lower = self.sql_table_name_to_table_desc_str_dict[this_sql_table_name].lower()

        #    if this_sql_table_name.lower() in self.sample_prompt_str:
        #        self.candidate_sql_table_list.append(this_sql_table_name)

        self.candidate_sql_table_list.append('xc_commission')
    

    def filter_join_possibilities(self):
        
        for this_join_sentence_str in self.join_table_description_list:

            for this_candidate_sql_table in self.candidate_sql_table_list:

                if this_candidate_sql_table in this_join_sentence_str:
                    self.final_join_table_description_list.append(this_join_sentence_str)

        #eliminate dups
        self.final_join_table_description_list = list(set(self.final_join_table_description_list))

        #convert to str
        self.final_join_table_description_str = '\n'.join(self.final_join_table_description_list)
        
    
    def build_final_prompt(self):
        
        #build sql_table_schema context str
        sql_table_context_str = ""

        for this_candidate_sql_table in self.candidate_sql_table_list:

            this_sql_table_desc = self.sql_table_name_to_table_desc_str_dict[this_candidate_sql_table]
            sql_table_context_str += this_sql_table_desc + "\n\n"

        #add to bottom of sql_table_context_str join_table_description_str
        sql_table_context_str += "\n\n" + self.final_join_table_description_str

        #substitute {user_question} with sample_prompt_str
        self.final_prompt_str = self.final_prompt_str.replace("{user_question}", self.sample_prompt_str)

        #substitute {table_metadata_string} with sql_table_context_str
        self.final_prompt_str = self.final_prompt_str.replace("{table_metadata_string}", sql_table_context_str)
   
        print(self.final_prompt_str)
    
    
    def get_final_prompt_str(self):
        return self.final_prompt_str