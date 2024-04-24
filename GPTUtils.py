import openai
import sys

class GPTUtils:
    
    def __init__(self, openai_key, background_context_str=""):

        self.openai_key = openai_key
        self.background_context_str = background_context_str
        self.prompt_text = ""
    
    def call_gpt(self, prompt_text="", call_gpt_turbo=True):
        
        if call_gpt_turbo == True:
            answer_text = self.call_gpt_turbo(prompt_text)
        else:
            answer_text = self.call_gpt_4(prompt_text)
        
        return answer_text
        

    def call_gpt_4(self, prompt_text):

        message=[{"role": "system", "content": self.background_context_str},
                {"role": "user", "content": prompt_text}]


        response = openai.ChatCompletion.create(
        model='gpt-4',
        temperature=0.01,
        max_tokens=2000,
        messages=message,
        frequency_penalty=0,
        presence_penalty=0
        )

        answer_text = response.choices[0].message.content

        return answer_text


    def call_gpt_turbo(self, prompt_text):

        message=[{"role": "system", "content": self.background_context_str},
                  {"role": "user", "content": prompt_text}]

        response = openai.ChatCompletion.create(
        model='gpt-3.5-turbo-0613',   #gpt-3.5-turbo   #gpt-3.5-turbo-0301  #gpt-4
        temperature=0,
        max_tokens=2000,
        messages=message,
        frequency_penalty=0,
        presence_penalty=0
        )

        answer_text = response.choices[0].message.content

        return answer_text


#these are variables suited for a QueryCache object to make GPT calls for different purposes
MIN_MAX_BACKGROUND_CONTEXT_STR = """Classify the following text into 1 of 2 possible categories: (1): MIN, if the text is asking for a minimum of something or 
(2): MAX, if the text is asking for a maximum of something. Only reply with either MIN or MAX, without using any other words. The text is:"""

ROW_ORDER_CONTEXT_STR = """Classify the following text into 1 of 2 possible categories: (1): DESC, if the text is asking for the top N results, the highest of something, or wants the query
to order results from highest to lowest (2): ASC, if the text is asking for the bottom N results, the lowest of something, or wants to order results from the lowest to highest. If neither is specified, return
ASC by default. Only reply with either ASC or DESC, without using any other words. If the question is not related to SQL, reply with ASC. Here are some examples.

Example 1: Who on my team had the highest total attainment in Q2 2015?
Answer: DESC

Example 2: Who on my team had the lowest total attainment in Q1 2020?
Answer: ASC

Example 3: Who on Mary Johnson's team had the highest total attainment in Q4 2018?
Answer: DESC

Example 4: Who on Jeffrey Partyka's team had the lowest total commissions in Q3 2016?
Answer: ASC

The text is: """

LIMIT_CLAUSE_CONTEXT_STR = """Determine the number of rows that should be returned by the following text. Only reply with an integer, if the number of rows to be returned in clearly specified. 
Do not return any other words in the response. If you cannot determine a number, reply with "nothing". Here are some examples.

Example 1: Who on my team had the highest total attainment in Q2 2015?
Answer: 1

Example 2: Who on my team had the lowest total attainment in Q1 2020?
Answer: 1

Example 3: Give me my top 5 commission amounts.
Answer: 5

Example 4: Who are the top 3 commission earners on Jeffrey Partyka's team in Q3 2016?
Answer: 3

Example 5: Who are the 10 worst commission earners on Jeffrey Partyka's team in Q3 2019?
Answer: 10

The text is:"""