import re
import os
import sys
import random
import spacy

class AnonymizePrompt:
    
    def __init__(self, user_question_str, participant_id, period_id_list, position_id_list):
        
        #different forms of the prompt
        self.orig_prompt = user_question_str
        self.curr_prompt = user_question_str
        self.anonymized_prompt = ""
        self.nlp = spacy.load("en_core_web_sm")
        
        #data structures for user question deanonymizing
        self.anon_phone_number_to_orig_phone_number_dict = {}
        self.anon_name_to_orig_name_dict = {}
        self.anon_id_to_orig_id_dict = {}
        self.anon_ssn_to_orig_ssn_dict = {}
        self.anon_email_to_orig_email_dict = {}
        
        #data structures for sql context (de)anonymizing
        self.anon_table_name_to_real_table_name_dict = {}
        self.commissions_anon_column_name_to_real_column_name_dict = {}
        self.user_anon_column_name_to_real_column_name_dict = {}
        self.participant_anon_column_name_to_real_column_name_dict = {}
        self.period_anon_column_name_to_real_column_name_dict = {}
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict = {}
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict = {}
        
        #data structures for id value anonymization
        self.participant_id = participant_id
        self.period_id_list = period_id_list
        self.position_id_list = position_id_list
        self.anon_participant_id_to_real_participant_id_dict = {}
        self.anon_period_id_to_real_period_id_dict = {}
        self.anon_position_id_to_real_position_id_dict = {}
        
        
        #start by anonymizing table and column names
        self.anonymize_tables()
        self.anonymize_columns()
        self.anonymize_ppp()
    
    
    def anonymize_tables(self):
        
        self.anon_table_name_to_real_table_name_dict['mydb_compensation'] = 'xc_commission'
        self.anon_table_name_to_real_table_name_dict['mydb_operator'] = 'xc_user'
        self.anon_table_name_to_real_table_name_dict['mydb_contributor'] = 'xc_participant'
        self.anon_table_name_to_real_table_name_dict['mydb_interval'] = 'xc_period'
        self.anon_table_name_to_real_table_name_dict['mydb_pecking_order'] = 'xc_pos_hierarchy'
        self.anon_table_name_to_real_table_name_dict['mydb_pecking_order_part_assignment'] = 'xc_pos_part_assignment'
        
    
    def anonymize_columns(self):
        
        #we will populate the sql schema related data structures
        self.commissions_anon_column_name_to_real_column_name_dict['xid'] = 'commission_id'
        self.commissions_anon_column_name_to_real_column_name_dict['xmoniker'] = 'name'
        self.commissions_anon_column_name_to_real_column_name_dict['xrevenue'] = 'amount'
        self.commissions_anon_column_name_to_real_column_name_dict['xinterval_id'] = 'period_id'
        self.commissions_anon_column_name_to_real_column_name_dict['xinterval_name'] = 'period_name'
        self.commissions_anon_column_name_to_real_column_name_dict['xcontributor_id'] = 'participant_id'
        self.commissions_anon_column_name_to_real_column_name_dict['xcontributor_name'] = 'participant_name'
        self.commissions_anon_column_name_to_real_column_name_dict['xrole_id'] = 'position_id'
        self.commissions_anon_column_name_to_real_column_name_dict['xrole_name'] = 'position_name'
        self.commissions_anon_column_name_to_real_column_name_dict['xacheivement_value'] = 'attainment_value'
        self.commissions_anon_column_name_to_real_column_name_dict['xacheivement_value_unit_type'] = 'attainment_value_unit_type'
        self.commissions_anon_column_name_to_real_column_name_dict['xclient_id'] = 'customer_id'
        self.commissions_anon_column_name_to_real_column_name_dict['xclient_name'] = 'customer_name'

        self.user_anon_column_name_to_real_column_name_dict['xoperator_id'] = 'user_id'
        self.user_anon_column_name_to_real_column_name_dict['xemessage'] = 'email'
        self.user_anon_column_name_to_real_column_name_dict['xoperator_name'] = 'name'
        
        self.participant_anon_column_name_to_real_column_name_dict['xcontributor_id'] = 'participant_id'
        self.participant_anon_column_name_to_real_column_name_dict['xmoniker'] = 'name'
        self.participant_anon_column_name_to_real_column_name_dict['xfirst_moniker'] = 'first_name'
        self.participant_anon_column_name_to_real_column_name_dict['xmiddle_moniker'] = 'middle_name'
        self.participant_anon_column_name_to_real_column_name_dict['xlast_moniker'] = 'last_name'
        self.participant_anon_column_name_to_real_column_name_dict['xworker_id'] = 'employee_id'
        self.participant_anon_column_name_to_real_column_name_dict['xwage'] = 'salary'
        self.participant_anon_column_name_to_real_column_name_dict['xoperator_id'] = 'user_id'
        
        self.period_anon_column_name_to_real_column_name_dict['xinterval_id'] = 'period_id'
        self.period_anon_column_name_to_real_column_name_dict['xmoniker'] = 'name'
        self.period_anon_column_name_to_real_column_name_dict['xbegin_date'] = 'start_date'
        self.period_anon_column_name_to_real_column_name_dict['xfinish_date'] = 'period_id'
        self.period_anon_column_name_to_real_column_name_dict['xparent_interval_id'] = 'parent_period_id'
        
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xpo_id'] = 'pos_hierarchy_id'
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xfrom_po_id'] = 'from_pos_id'
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xfrom_po_moniker'] = 'from_pos_name'
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xto_po_id'] = 'to_pos_id'
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xto_po_moniker'] = 'to_pos_name'
        self.pos_hierarchy_anon_column_name_to_real_column_name_dict['xpo_type_id'] = 'pos_hierarchy_type_id'
        
        
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict['xpecking_order_part_assignment_id'] = 'pos_part_assignment_id'
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict['xcontributor_id'] = 'participant_id'
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict['xcontributor_name'] = 'participant_name'
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict['xrole_id'] = 'position_id'
        self.pos_part_assignment_anon_column_name_to_real_column_name_dict['xrole_name'] = 'position_name'
        
        
    def anonymize_ppp(self):
        
        #start with participant_id
        self.anon_participant_id_to_real_participant_id_dict["participant_id_0"] = self.participant_id
        
        #position id
        if self.position_id_list is not None:
            for position_idx, this_position_id in enumerate(self.position_id_list):
                self.anon_position_id_to_real_position_id_dict["position_id_" + str(position_idx)] = this_position_id
        
        #period_id
        if self.period_id_list is not None:
            for period_idx, this_period_id in enumerate(self.period_id_list):
                self.anon_period_id_to_real_period_id_dict["period_id_" + str(period_idx)] = this_period_id
        
        
        
    def anonymize_prompt(self):
        
        #anonymize id values
        self.anonymize_ids()
        
        #anonymize phone numbers
        self.anonymize_phone_numbers()
        
        #anonymize person and company names
        self.anonymize_names()
        
        #anonymize ssn values
        self_curr_prompt = self.anonymize_ssns()
        
        self.anonymized_prompt = self.curr_prompt
        
        #finally, let's add the anonymized id values included in the request to this web service to self.curr_prompt
        participant_id_sentence = " The participant_id to be used is: " + str(list(self.anon_participant_id_to_real_participant_id_dict.keys())[0]) + ". "
        position_id_sentence = ""
        period_id_sentence = ""
        
        if self.position_id_list is not None:
        
            position_id_sentence = "The position_id values to be used are: "

            comma = ""

            for this_anon_position_id in self.anon_position_id_to_real_position_id_dict.keys():
                position_id_sentence += comma + str(this_anon_position_id)
                comma = ","

            position_id_sentence += ". "
        
        if self.period_id_list is not None:
        
            period_id_sentence = "The period id values to be used are: "

            comma2 = ""

            for this_anon_period_id in self.anon_period_id_to_real_period_id_dict.keys():
                period_id_sentence += comma2 + str(this_anon_period_id)
                comma2 = ","

            period_id_sentence += ". "
        
        self.anonymized_prompt += participant_id_sentence + position_id_sentence + period_id_sentence
        
        return self.anonymized_prompt
    
    
    def anonymize_ids(self):
        
        #we will anonymize any id that is >= 5 digits in length
        
        id_regex = r'(\d{5,})[.?!]?'
        
        # Find all matches of id values in the text
        id_value_list = re.findall(id_regex, self.curr_prompt)
        
        for ididx, this_id_value_str in enumerate(id_value_list):
            
            #replace this_id_value_str with a random id
            self.curr_prompt = re.sub(this_id_value_str, "id_" + str(ididx), self.curr_prompt)
            
            self.anon_id_to_orig_id_dict["id_" + str(ididx)] = this_id_value_str
            
    
    def anonymize_phone_numbers(self):
        
        phone_number_regex = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        
        # Find all matches of phone numbers in the text
        phone_number_list = re.findall(phone_number_regex, self.curr_prompt)
                
        for this_phone_number in phone_number_list:
                        
            # Extract the format (e.g., "123-456-7890" without non-digit characters)
            format_string = re.sub(r'\D', '', this_phone_number)
            
            # Generate a new set of digits with the same length as the format
            new_digits = ''.join(str(random.randint(0, 9)) for _ in format_string)
            
            # Create the new phone number with the desired format
            anon_phone_number = re.sub(r'\d', lambda x: new_digits[x.start()], format_string)
                        
            # Add dash characters back to the new phone number, so that we can distinguish this from numeric id values
            #we will add 2 dashes - one after the 3rd digit, and the next after the 6th digit
            anon_phone_number = anon_phone_number[:3] + '-' + anon_phone_number[3:6] + '-' + anon_phone_number[6:]
            
            self.anon_phone_number_to_orig_phone_number_dict[anon_phone_number] = this_phone_number
            
            #replace orig phone number with anon phone number
            self.curr_prompt = re.sub(this_phone_number, anon_phone_number, self.curr_prompt)
            
    
    def anonymize_names(self):
        
        # Process the text with spaCy
        doc = self.nlp(self.curr_prompt)
        
        for entityidx, this_entity in enumerate(doc.ents):
            if this_entity.label_ == "PERSON":
                self.anon_name_to_orig_name_dict["Jane Smith_" + str(entityidx)] = this_entity.text
                self.curr_prompt = re.sub(this_entity.text, "Jane Smith_" + str(entityidx), self.curr_prompt)
                
            if this_entity.label_ == "ORG":
                self.anon_name_to_orig_name_dict["Org_" + str(entityidx)] = this_entity.text
                self.curr_prompt = re.sub(this_entity.text, "Org_" + str(entityidx), self.curr_prompt)
    
    def anonymize_ssns(self):
        
        ssn_number_regex = r'^(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}$'
        
        # Find all matches of phone numbers in the text
        ssn_list = re.findall(ssn_number_regex, self.curr_prompt)    
    
        for ssnidx, this_ssn in enumerate(ssn_list, 1):
            anon_ssn = "ssn_" + str(ssnidx)
            
            self.anon_ssn_to_orig_ssn_dict[anon_ssn] = this_ssn
            
            self.curr_prompt = re.sub(this_ssn, anon_ssn, self.curr_prompt)
    
    def anonymize_emails(self):
        
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        email_list = re.findall(email_regex, self.curr_prompt)
        
        for emailidx, this_email in enumerate(email_list):
            anon_email = "email_" + str(emailidx)
            
            self.anon_email_to_orig_email_dict[anon_email] = this_email
            
            self.curr_prompt = re.sub(this_email, anon_email, self.curr_prompt)
    
    
    def deanonymize_answer(self, this_anon_answer_str):
        
        #we have to deanonymize BOTH the user question and the context that goes with it
        #start with the user question
        
        #phone numbers
        for this_anon_phone_number, this_real_phone_number in self.anon_phone_number_to_orig_phone_number_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_phone_number, this_real_phone_number)
        
        #names (people and companies)
        for this_anon_name, this_real_name in self.anon_name_to_orig_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_name, this_real_name)
        
        #id values
        for this_anon_id_value, this_real_id_value in self.anon_id_to_orig_id_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_id_value, this_real_id_value)
            
        #ssn values
        for this_anon_ssn_value, this_real_ssn_value in self.anon_ssn_to_orig_ssn_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_ssn_value, this_real_ssn_value)
        
        #emails
        for this_anon_email, this_real_email in self.anon_email_to_orig_email_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_email, this_real_email)
        
        #now deanonymize the sql context, including tables and columns
        #start with xc_commissions table
        
        for this_anon_column_name, this_real_column_name in self.commissions_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)

        #xc_user        
        for this_anon_column_name, this_real_column_name in self.user_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)
        
        #xc_participant
        for this_anon_column_name, this_real_column_name in self.participant_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)
        
        #xc_period
        for this_anon_column_name, this_real_column_name in self.period_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)
            
        #xc_pos_hierarchy
        for this_anon_column_name, this_real_column_name in self.pos_hierarchy_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)
        
        #xc_pos_hierarchy
        for this_anon_column_name, this_real_column_name in self.pos_part_assignment_anon_column_name_to_real_column_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_column_name, this_real_column_name)
        
        #deanonymize table names in this_anon_answer_str
        
        for this_anon_table_name, this_real_table_name in self.anon_table_name_to_real_table_name_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace(this_anon_table_name, this_real_table_name)
        
        #deanonymize period_id values, position_id values and the participant_id value
        #start with participant_id
        for this_anon_participant_id, this_real_participant_id in self.anon_participant_id_to_real_participant_id_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace("'" + str(this_anon_participant_id) + "'", str(this_real_participant_id))
        
        #position id values
        for this_anon_position_id, this_real_position_id in self.anon_position_id_to_real_position_id_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace("'" + str(this_anon_position_id)+ "'", str(this_real_position_id))
        
        #period_id values
        for this_anon_period_id, this_real_period_id in self.anon_period_id_to_real_period_id_dict.items():
            this_anon_answer_str = this_anon_answer_str.replace("'" + str(this_anon_period_id) + "'", str(this_real_period_id))
        
                                         
        return this_anon_answer_str