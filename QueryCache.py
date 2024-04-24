import os
import sys
import spacy
import re
import truecase
import openai

from sentence_transformers import SentenceTransformer, util
from spellchecker import SpellChecker

from GPTUtils import GPTUtils, MIN_MAX_BACKGROUND_CONTEXT_STR, ROW_ORDER_CONTEXT_STR, LIMIT_CLAUSE_CONTEXT_STR


class QueryCache:

    def __init__(self, user_question_str, period_id_list, participant_id, position_id_list, openai_api_key, query_to_threshold_dict, query_to_signature_dict_dict, query_to_sql_dict):
        
        #prompt fields
        self.orig_prompt = user_question_str
        self.curr_prompt = user_question_str
        
        #OpenAI API Key
        self.openai_api_key = openai_api_key
        
        #for company matching support
        self.company_set = set()
        
        #a list of companies to match against
        with open("company_list.txt", "r") as readfile:
            self.company_set = set(readfile.read().split('\n'))
        
        self.company_acronym_set = set()
        
        with open("company_acronyms.txt", "r") as readfile2:
            self.company_acronym_set = set(readfile2.read().split('\n'))
        
        #for prompt sentence splitting
        self.alphabets = "([A-Za-z])"
        self.prefixes = "(Mr|St|Mrs|Ms|Dr)[.]"
        self.suffixes = "(Inc|Ltd|Jr|Sr|Co)"
        self.starters = "(Mr|Mrs|Ms|Dr|He\s|She\s|It\s|They\s|Their\s|Our\s|We\s|But\s|However\s|That\s|This\s|Wherever)"
        self.acronyms = "([A-Z][.][A-Z][.](?:[A-Z][.])?)"
        self.websites = "[.](com|net|org|io|gov)"
        
        #entity detection
        self.nlp = spacy.load("en_core_web_sm")
        self.doc = None
        self.date_entity_set = set()
        self.person_entity_set = set()
        self.org_entity_set = set()
        self.date_entity_count = 0
        self.person_entity_count = 0
        self.org_entity_count = 0
        self.participant_id = participant_id
        self.period_id_list = period_id_list
        self.position_id_list = position_id_list
        self.curr_prompt_entity_signature_dict = {}
        self.matching_entitized_query_list = []
        
        #semantic similarity
        self.query_to_threshold_dict = query_to_threshold_dict
        self.query_to_signature_dict_dict = query_to_signature_dict_dict
        self.query_to_sql_dict = query_to_sql_dict
        self.embedding_model_name = "all-MiniLM-L6-v2"
        self.embedding_function = SentenceTransformer(self.embedding_model_name)
        
        #function calls
        self.get_true_case()
        sentences = self.split_into_sentences(self.orig_prompt)
        
        print("After split_into_sentences: sentences are: " + str(sentences))
        
        self.assign_prompt_sentences(sentences)
        self.entitize_user_query()
        self.correct_misspellings()
        self.create_entity_signature()
        self.match_query()
    
    
    def get_current_prompt(self):
        return self.curr_prompt
    
    
    def get_true_case(self):
        
        #the user prompt may or may not use correct cases in their prompt
        #however, not using the correct case for names may prevent entity detection of names and companies in the prompt
        #thus, we will use truecase to convert the words in the prompt to the correct case
        
        self.orig_prompt = self.curr_prompt = truecase.get_true_case(self.orig_prompt)
        
        #I need to check if any words in self.orig_prompt are company acronyms that can be found in self.company_acronym_set
        #TODO
        
    def split_into_sentences(self, text):
                
        text = " " + text + "  "
        text = text.replace("\n"," ")
        text = re.sub(self.prefixes,"\\1<prd>",text)
        text = re.sub(self.websites,"<prd>\\1",text)

        if "Ph.D" in text: text = text.replace("Ph.D.","Ph<prd>D<prd>")
        text = re.sub("\s" + self.alphabets + "[.] "," \\1<prd> ",text)
        text = re.sub(self.acronyms+" "+self.starters,"\\1<stop> \\2",text)
        text = re.sub(self.alphabets + "[.]" + self.alphabets + "[.]" + self.alphabets + "[.]","\\1<prd>\\2<prd>\\3<prd>",text)
        text = re.sub(self.alphabets + "[.]" + self.alphabets + "[.]","\\1<prd>\\2<prd>",text)
        text = re.sub(" "+self.suffixes+"[.] "+self.starters," \\1<stop> \\2",text)
        text = re.sub(" "+self.suffixes+"[.]"," \\1<prd>",text)
        text = re.sub(" " + self.alphabets + "[.]"," \\1<prd>",text)        
        if "”" in text: text = text.replace(".”","”.")
        if "\"" in text: text = text.replace(".\"","\".")
        if "!" in text: text = text.replace("!\"","\"!")
        if "?" in text: text = text.replace("?\"","\"?")
                
        text = text.replace(".",".<stop>")
        text = text.replace("?","?<stop>")
        text = text.replace("!","!<stop>")
        text = text.replace("<prd>",".")
        sentences = text.split("<stop>") 
        
        sentences = [sentences[0]]
        
        #sentences = sentences[:-1]
        
        sentences = [s.strip() for s in sentences]
        
        return sentences
    
    
    def assign_prompt_sentences(self, sentences):

        #assign only first sentence to self.curr_prompt 
        self.curr_prompt = sentences[0]
        
        #set self.doc for entity extraction
        self.doc = self.nlp(self.curr_prompt)

        
    
    def get_company_entities_from_list(self, sanitized_prompt_str):
        
        #remove all punctuation symbols
        pattern_str = r'[\?\.,!:;=\-\{\}\(\)\[\]\$\*\+\|\^]'       
        sanitized_prompt_str = re.sub(pattern_str, '', sanitized_prompt_str)
        
        found_companies_list = []
                
        for this_company_str in self.company_set:
            
            if this_company_str in sanitized_prompt_str:
                found_companies_list.append(this_company_str)
                    
        return found_companies_list    
        
    
    def entitize_user_query(self):
        
        self.date_entity_set = set((ent.text) for ent in self.doc.ents if ent.label_ == "DATE")
        self.person_entity_set = set((ent.text) for ent in self.doc.ents if ent.label_ == "PERSON")
        self.org_entity_set = set((ent.text) for ent in self.doc.ents if ent.label_ == "ORG")
                
        company_entity_list = self.get_company_entities_from_list(self.curr_prompt)
        
        self.org_entity_set.update(company_entity_list)
        
        #remove empty space entities - this is a known bug in Spacy
        self.org_entity_set = {this_entity for this_entity in self.org_entity_set if this_entity}
        self.person_entity_set = {this_entity for this_entity in self.person_entity_set if this_entity}
        self.date_entity_set = {this_entity for this_entity in self.date_entity_set if this_entity}
        
        #remove 's from person_entity, as the possessive 's should not be part of any person name
        self.person_entity_set = { this_person_entity.replace("'s","") for this_person_entity in self.person_entity_set }

        self.date_entity_count = len(self.date_entity_set)
        self.person_entity_count = len(self.person_entity_set)
        self.org_entity_count = len(self.org_entity_set)
        
        date_entity_cnt = 1
        
        #we have to use re.sub() instead of str.replace because() entities in the prompt may be uppercase or lowercase
        #only re.sub() can perform case-insensitive matches

        for this_date_entity in self.date_entity_set:
            self.curr_prompt = re.sub(this_date_entity, "date_entity_" + str(date_entity_cnt), self.curr_prompt, flags=re.IGNORECASE)
            date_entity_cnt += 1

        person_entity_cnt = 1

        for this_person_entity in self.person_entity_set:
            self.curr_prompt = re.sub(this_person_entity, "person_entity_" + str(person_entity_cnt), self.curr_prompt, flags=re.IGNORECASE)
            person_entity_cnt += 1

        org_entity_cnt = 1

        for this_org_entity in self.org_entity_set:
            self.curr_prompt = re.sub(this_org_entity, "org_entity_" + str(org_entity_cnt), self.curr_prompt, flags=re.IGNORECASE)
            org_entity_cnt += 1
    
    
    def create_entity_signature(self):
        
        #we need to create an entity signature from self.curr_prompt
        #start with our counts
        
        self.curr_prompt_entity_signature_dict["date_count"] = self.date_entity_count
        self.curr_prompt_entity_signature_dict["person_count"] = self.person_entity_count
        self.curr_prompt_entity_signature_dict["org_count"] = self.org_entity_count
        
        #for now, these will be empty
        or_list = []
        and_list = []
        
        self.curr_prompt_entity_signature_dict["or"] = or_list
        self.curr_prompt_entity_signature_dict["and"] = and_list
        
        #print(self.curr_prompt_entity_signature_dict)
    
    
    def correct_misspellings(self):
        
        spell = SpellChecker()
        
        #make a copy of self.curr_prompt
        curr_prompt_copy = str(self.curr_prompt)
        
        #remove puncutation from curr_prompt_copy
        punctuation_regex_str = r'(\?|!|\.)'
        curr_prompt_copy = re.sub(punctuation_regex_str, "", curr_prompt_copy)
        
        #find those words that may be misspelled in self.curr
        questionable_words_list = spell.unknown(curr_prompt_copy.split())
                
        #we are not going to include entities in our spell check
        person_entity_regex_str = r'person_entity_(\d)+'
        org_entity_regex_str = r'org_entity_(\d)+'
        date_entity_regex_str = r'date_entity_(\d)+'
        
        #this is a mapping of misspelled word to correctly spelled word
        misspelled_word_to_correct_word_dict = {}
        
        for this_questionable_word in questionable_words_list:
            
            if not bool(re.search(person_entity_regex_str, this_questionable_word, re.IGNORECASE)) and not bool(re.search(org_entity_regex_str, this_questionable_word, re.IGNORECASE)) and not bool(re.search(date_entity_regex_str, this_questionable_word, re.IGNORECASE)):
                
                #get correct spelling
                this_corrected_word = spell.correction(this_questionable_word)
                
                #add to mapping
                misspelled_word_to_correct_word_dict[this_questionable_word] = this_corrected_word
        
        #add correctly spelled words to self.curr_prompt
        for this_questionable_word, this_correct_word in misspelled_word_to_correct_word_dict.items():
            curr_prompt_copy = re.sub(this_questionable_word, this_correct_word, curr_prompt_copy, flags=re.IGNORECASE)
        
        #add initial capitalization for the first word in the sentence
        curr_prompt_copy = curr_prompt_copy.capitalize()
        
        #finally, add ? to end of prompt
        curr_prompt_copy = curr_prompt_copy + '?'
        
        self.curr_prompt = curr_prompt_copy
        
    
    def match_query(self):
        
        #we need to match the entitized user query against all entity signatures present in self.query_to_signature_dict_dict
        print("query signature is: " + str(self.curr_prompt_entity_signature_dict))
        
        for this_entitized_query, this_entity_signature_dict in self.query_to_signature_dict_dict.items():
            
            if self.entity_counts_match(this_entity_signature_dict, self.curr_prompt_entity_signature_dict) == True and self.boolean_lists_match(this_entity_signature_dict, self.curr_prompt_entity_signature_dict) == True:
                
                self.matching_entitized_query_list.append(this_entitized_query)
    
    def get_curr_entity_signature(self):
        return self.curr_prompt_entity_signature_dict
                                  
    
    def get_matching_entitized_query_list(self):
        return self.matching_entitized_query_list
            
    
    def entity_counts_match(self, this_entity_signature_dict, curr_prompt_entity_signature_dict):
        
        #print("this_entity_signature_dict is: " + str(this_entity_signature_dict))
        #print("curr_prompt_entity_signature_dict is: " + str(curr_prompt_entity_signature_dict))
        #print('--------')
        
        if this_entity_signature_dict["date_count"] == curr_prompt_entity_signature_dict["date_count"] and this_entity_signature_dict["person_count"] == curr_prompt_entity_signature_dict["person_count"] and this_entity_signature_dict["org_count"] == curr_prompt_entity_signature_dict["org_count"]:
            print("Entity counts match")
            return True
        else:
            print("Entity counts DO NOT match")
            return False

        
    def boolean_lists_match(self, this_entity_signature_dict, curr_prompt_entity_signature_dict):
        
        if this_entity_signature_dict["or"] == curr_prompt_entity_signature_dict["or"] and this_entity_signature_dict["and"] == curr_prompt_entity_signature_dict["and"]:
            print("Boolean counts match")
            return True
        else:
            print("Boolean counts DO NOT match")
            return False
    
    
    def get_semantic_matching_query(self):
        
        #we need to determine the semantic similarity between self.curr_prompt and self.matching_entitized_query_list.
        #first, let's address the case where a query did not match any of the entity templates. In this case, we will just send the query to the LLM
        
        if len(self.matching_entitized_query_list) == 0:
            return "",""
        
        #If we get here, that means we have at least 1 matching entity template. First, turn both into embeddings
        
        curr_prompt_embedding = self.embedding_function.encode(self.curr_prompt, convert_to_tensor=True)
        
        matching_entitized_query_to_sim_score_dict = {}
        
        for this_matching_entitized_query in self.matching_entitized_query_list:
            
            this_matching_entitized_query_embedding = self.embedding_function.encode(this_matching_entitized_query, convert_to_tensor=True)
            this_sim_score = round(util.cos_sim(curr_prompt_embedding, this_matching_entitized_query_embedding)[0][0].item(), 3)
            matching_entitized_query_to_sim_score_dict[this_matching_entitized_query] = this_sim_score
            
            print("this sim score is: " + str(this_sim_score))
        
        best_matching_entitized_query = max(matching_entitized_query_to_sim_score_dict, key=lambda k: matching_entitized_query_to_sim_score_dict[k])
        
        #if best_matching_entitized_query has a sim score >= the query's threshold, then de-entitize and return the query
        #if best_matching_entitized_query has a sim score < the relevance threshold, then return a warning to the user
        #if best_matching_entitized_query >= the relevance threshold but < the query's threshold, then pass it off to the LLM
        
        best_matching_query_sim_score = matching_entitized_query_to_sim_score_dict[best_matching_entitized_query]
        
        #print("best matching entitized query is: " + best_matching_entitized_query)
        #print("best matching query sim score is: " + str(best_matching_query_sim_score))
        #print("curr prompt = " + str(self.curr_prompt))
        
        if best_matching_query_sim_score >= self.query_to_threshold_dict[best_matching_entitized_query]:
            
            return_query = self.deentitize_query(best_matching_entitized_query)
            
            #determine correct MAX/MIN functions to be in the final query
            return_query = self.process_max_min_operators(return_query)
            
            #determine correct result order - but only for queries with an ORDER BY clause already there
            order_by_pattern_str = r'ORDER( )+BY( )+[A-Za-z0-9_\.]+(( )+(ASC|DESC))?'
            
            if bool(re.search(order_by_pattern_str, return_query, re.IGNORECASE)) == True:
                return_query = self.process_result_row_order(return_query)
            
            #create the correct number of rows to add to the LIMIT clause
            limit_pattern_str = r'LIMIT( )+\d+'
            
            if bool(re.search(limit_pattern_str, return_query, re.IGNORECASE)) == True:
                return_query = self.process_limit_rows(return_query)
                        
            return return_query, best_matching_entitized_query
        
        elif best_matching_query_sim_score < self.query_to_threshold_dict[best_matching_entitized_query]:
            return "This query cannot be answered by our existing LLM functionality.",""
        else:
            
            #we have to pass this query to the LLM - but before we do, we need to anonymize the participant_id, position_ids and period_ids
            #write code to do this here
            
            
            return "",""
    
    
    def process_max_min_operators(self, return_query):
        
        #if return query has either a MAX or MIN SQL operator, call GPT to determine which is appropriate given the request
        if "MAX(" in return_query or "MIN(" in return_query:

            #create GPTUtils object
            gpt_obj = GPTUtils(self.openai_api_key, MIN_MAX_BACKGROUND_CONTEXT_STR)
            gpt_max_min_response = gpt_obj.call_gpt(self.curr_prompt)

            pattern_str = r'MAX\(|MIN\('
            return_query = re.sub(pattern_str, gpt_max_min_response + "(", return_query)

        return return_query
    
    
    def process_result_row_order(self, return_query):
        
        gpt_row_order_obj = GPTUtils(self.openai_api_key, ROW_ORDER_CONTEXT_STR)
        
        gpt_row_order_response = gpt_row_order_obj.call_gpt(self.curr_prompt).strip()
        
        #print("self.orig_prompt = " + self.orig_prompt)
        
        #determine if an ORDER BY clause is in the cached query. Only letters, numbers and the underscore character are legal characters for column names
        #don't forget to include a period if we are qualifying the column name with a period
        
        order_by_pattern_str = r'ORDER( )+BY( )+[A-Za-z0-9_\.]+(( )+(ASC|DESC))?'
        limit_pattern_str = r'LIMIT( )+(\d)+'
        first_selected_column_pattern_str = r'SELECT( )+([A-Za-z0-9_\.]+)'
        
        #we will only specify order explicitly only if results should be in DESC order, since ASC is set by default in SQL
        if gpt_row_order_response == "DESC":
            gpt_order_by_order_str = "DESC"
        else:
            gpt_order_by_order_str = "ASC"
        
        first_selected_column_str = re.search(first_selected_column_pattern_str, return_query, re.IGNORECASE).group(2)
        
        #if order by clause is not there, then we add ORDER BY first_column gpt_order_by_order_str
        if not bool(re.search(order_by_pattern_str, return_query, re.IGNORECASE)):
            
            #I will assume here that there is no ORDER BY clause in return_query
            #I will assume here that we will order results by the first column selected
            #now I need to figure out where to put this
            
            #if there is not a limit clause, then put ORDER BY clause at the end of return_query
            if not bool(re.search(limit_pattern_str, return_query, re.IGNORECASE)):
                return_query = return_query + " ORDER BY " + first_selected_column_str + " " + gpt_order_by_order_str
            else:
                
                #there is a limit clause, so we will have to insert it before it
                #first, get the limit clause
                
                limit_clause_str = re.search(limit_pattern_str, return_query, re.IGNORECASE).group(0)
                
                return_query = re.sub(limit_clause_str, "ORDER BY " + first_selected_column_str + " " + gpt_order_by_order_str + " " + limit_clause_str, return_query, flags=re.IGNORECASE)

        else:
            
            #print("Order by clause is there...")
            
            #the order by clause is there, so we need to verify it and if necessary, make a change
            order_by_clause_str = re.search(order_by_pattern_str, return_query, re.IGNORECASE).group(0)
            
            #print("order_by_clause_str=" + order_by_clause_str)
            
            asc_desc_pattern_str = r'ASC|DESC'
            
            query_order_by_order_str = re.search(asc_desc_pattern_str, order_by_clause_str, re.IGNORECASE).group(0)
            
            #print("query_order_by_order_str=" + query_order_by_order_str)
            
            if query_order_by_order_str == "":
                query_order_by_order_str = "ASC"
            
            #print("query_order_by_order_str after if stmt is = " + query_order_by_order_str)
            
            #print("gpt_order_by_order_str = " + gpt_order_by_order_str)
            #print("gpt_row_order_response = " + gpt_row_order_response)   
        
            #if GPT's row order is different then what is in order_by_clause_str, then make a change
            if gpt_order_by_order_str == "DESC" and query_order_by_order_str == "ASC":

                #yes, add a DESC
                return_query = re.sub(order_by_clause_str, order_by_clause_str + " DESC ", return_query, flags=re.IGNORECASE)
            
            
            if gpt_order_by_order_str == "ASC" and query_order_by_order_str == "DESC":
         
                #remove a DESC
                order_by_clause_new_str = re.sub("DESC", "", order_by_clause_str, flags=re.IGNORECASE)
                
                return_query = re.sub(order_by_clause_str, order_by_clause_new_str, return_query, flags=re.IGNORECASE)
        
        return return_query
    
    
    def process_limit_rows(self, return_query):
        
        #we need to determine the number of rows that should be part of the limit clause
        limit_pattern_str = r'LIMIT( )+\d+'
            
        #get limit clause match obj
        limit_clause_match_str = re.search(limit_pattern_str, return_query, re.IGNORECASE).group(0)
        
        gpt_limit_rows_obj = GPTUtils(self.openai_api_key, LIMIT_CLAUSE_CONTEXT_STR)
        
        gpt_limit_rows_response = gpt_limit_rows_obj.call_gpt(self.curr_prompt).strip()
        
        new_limit_clause_str = "LIMIT " + str(gpt_limit_rows_response)
        
        #finally, replace limit_clause_match_str with new_limit_clause_str
        return_query = re.sub(limit_clause_match_str, new_limit_clause_str, return_query, flags=re.IGNORECASE)
        
        return return_query

    
    def deentitize_query(self, query_to_deentitize):
        
        #we need to get the sql query associated with query_to_deentitize
        sql_query_to_deentitize = self.query_to_sql_dict[query_to_deentitize]
        
        #start by dentitizing the dates
        for entityidx, this_date_entity in enumerate(self.date_entity_set, 1):
            sql_query_to_deentitize = sql_query_to_deentitize.replace('date_entity_' + str(entityidx), this_date_entity)
        
        #then people
        for entityidx, this_person_entity in enumerate(self.person_entity_set, 1):
            sql_query_to_deentitize = sql_query_to_deentitize.replace('person_entity_' + str(entityidx), this_person_entity)
        
        #then orgs
        for entityidx, this_org_entity in enumerate(self.org_entity_set, 1):
            sql_query_to_deentitize = sql_query_to_deentitize.replace('org_entity_' + str(entityidx), this_org_entity)
        
        #then period id values, if any
        if self.period_id_list is not None:
                                     
            sql_query_to_deentitize = sql_query_to_deentitize.replace('{period_id_list}', str(self.period_id_list))
            sql_query_to_deentitize = sql_query_to_deentitize.replace('([', '(')
            sql_query_to_deentitize = sql_query_to_deentitize.replace('])', ')')
            
        
        #then position_id values, if any
        if self.position_id_list is not None:
                                     
            sql_query_to_deentitize = sql_query_to_deentitize.replace('{position_id_list}', str(self.position_id_list))
            sql_query_to_deentitize = sql_query_to_deentitize.replace('([', '(')
            sql_query_to_deentitize = sql_query_to_deentitize.replace('])', ')')
        
        #then participant_id
        sql_query_to_deentitize = sql_query_to_deentitize.replace('{participant_id}', str(self.participant_id))
                                                                     
        #remove any linebreaks
        sql_query_to_deentitize = sql_query_to_deentitize.replace('\n','')
        
        return sql_query_to_deentitize