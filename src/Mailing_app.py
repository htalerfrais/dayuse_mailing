import os
import snowflake.connector
from dotenv import load_dotenv
import json
import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
from utils import prompt_templates

load_dotenv()
# Panda settings
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)  # or a specific number like 1000
pd.set_option('display.expand_frame_repr', False)

def get_snowflake_connection():  
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        host=os.getenv("SNOWFLAKE_HOST"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        )
    return conn

# code for the analyst app 
# configurate the used API
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "DAYUSE_ANALYTICS.STREAMLIT_APPS.STREAMLIT_STAGE/customer_360.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # in milliseconds

conn = get_snowflake_connection()
cur = conn.cursor()
#test connection




# ------------------------------------------------------------------------------------------------

def complete_prompts(prompt_template, sql_template, placeholder_value):
    # une fonction qui retourne une sql query constitué du prompt template complété par le placeholder et du system prompt
    formated_prompt = prompt_template.format(placeholder = placeholder_value)
    formated_prompt_sql = sql_template.format(placeholder = formated_prompt)
    
    return formated_prompt, formated_prompt_sql



# ------------------------------------------------------------------------------------------------


def get_analyst_response(conv: List[Dict]):

    request_body = {
        "messages": conv,
        "semantic_model_file": f"@{AVAILABLE_SEMANTIC_MODELS_PATHS[0]}",
        "stream": False, 
    }
    print(request_body['messages'])

    resp = requests.post(
        url=f"https://{conn.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{conn.rest.token}"',
            "Content-Type": "application/json",
        }
    )

    parsed_response = json.loads(resp.text)  
    
    print(f"Parsed_response from analyst: {parsed_response}")
    return parsed_response




def get_query_exec_result(analyst_response_json : dict):
    # puts the sql, the thought, the sql confidency, the dataframe at the right place
    # query_exec_result = [thought, sql_query, sql_confidence, df]
    
    query_exec_result=[]
    sql_query = ""
    sql_confidence = ""
    thought = ""
    content = analyst_response_json['message']['content']
    for item in content :
        if item['type'] == "sql":
            sql_query = item["statement"]
            sql_confidence = item["confidence"]
        elif item['type'] == "text":
            thought = item["text"]
    
    global session
    try:
        df = cur.execute(sql_query).fetchall()
        query_exec_result = [thought, sql_query, sql_confidence, df]
        return query_exec_result
    except SnowparkSQLException as e:
        return str(e)
    


# -------------------------------------- Utils -------------------------------------------------

    
def create_user_payload(user_text:str):    
    # Create the payload structure
    payload = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": user_text
                }
            ]
        }
    ]
    # Return the payload as a Python object
    return payload
    
def create_sql_llm_query(sql_template, placeholder, system_prompt):
    return sql_template.format(placeholder=placeholder, system_prompt=system_prompt)
    


# ----------------------------------- Utils iterate function --------------------------------------


def process_customer_ids(customer_tuples, prompt_template_reco):
    # gives hotel recommendation for each customer id
    hotel_reco = []
    for i, (customer_id, customer_name) in enumerate(customer_tuples):
        print(f"Processing customer {i}: {customer_id} ({customer_name})")
        
        # create the payload for a customer id
        placeholder_reco = prompt_template_reco.format(customer_id=customer_id)
        payload_reco = create_user_payload(placeholder_reco)
        print(payload_reco)
        
        # get response: hotel to recommend
        analyst_response_reco = get_analyst_response(payload_reco)
        print(f"Analyst response for hotel recommendation for customer {i}: {customer_id} ({customer_name}): \n {analyst_response_reco}")
        query_exec_result_hotel_reco = get_query_exec_result(analyst_response_reco)
        
        # query_exec_result = [thought, sql_query, sql_confidence, df] for hotel reco
        print("---------- hotel recommendation res ----------")
        print(f"thought:\n {query_exec_result_hotel_reco[0]}\n")
        print(f"sql_query:\n {query_exec_result_hotel_reco[1]}\n")
        print(f"sql_confidence:\n {query_exec_result_hotel_reco[2]}\n")
        print(f"df_hotel_recommendation:\n{query_exec_result_hotel_reco[3]}\n")
        print("----------------------------------------------")

        hotel_reco.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'hotel_recommendation': query_exec_result_hotel_reco[3]
        })

    return hotel_reco





def get_target_customers(prompt_template_target_customer):
    # get the customer ids of customers that went in a hotel in Paris, France during the past 7 days
    # and the first name of the customer
    payload_target_customer = create_user_payload(prompt_template_target_customer)
    analyst_response_target_customer = get_analyst_response(payload_target_customer)
    query_exec_result_target_customer = get_query_exec_result(analyst_response_target_customer)
    print(f"df customer targets: \n{query_exec_result_target_customer[3]}\n")

    return query_exec_result_target_customer



def get_hotel_recommendations(prompt_template_recommand, query_exec_result_target_customer):
    """
    Args : 
    prompt_template_recommand : prompt telling how to retrieve the hotels to recommand to each customer and the informations to retrieve
    query_exec_result_target_customer : result of the query that retrieves the customer ids and the first names of the customers
    
    Returns : 
    list_ids_rec : list of customer ids and the hotel recommendations corresponding 
    """

    # get the hotel recommendations for the target customers
    df_customer_target = query_exec_result_target_customer[3]
    list_ids_rec = process_customer_ids(df_customer_target, prompt_template_recommand)

    return list_ids_rec



def generate_batch_mail(list_customer_hotel, prompt_template_mail, prompt_template_SQL, system_prompt):
    # sql template mail is the sql query calling llm where we have to put the prompt 
    # prompt template mail is the template of the prompt sent to the llm where we have to add customer and hotel info
    # list_customer_hotel contains the customer info and hotel recommendations
    
    generated_mails = []
    for customer_data in list_customer_hotel:
        # Extract customer information
        customer_id = customer_data["customer_id"]
        customer_name = customer_data["customer_name"]
        hotel_recommendations = customer_data["hotel_recommendation"]
        
        # Format hotel recommendations for better readability in the prompt
        formatted_hotels = ""
        for i, hotel_info in enumerate(hotel_recommendations):
            formatted_hotels += f"Hotel {i+1}:\n"
            # Handle different tuple formats by checking length and types
            for j, item in enumerate(hotel_info):
                if j == 0:
                    formatted_hotels += f"- Name: {item}\n"
                elif isinstance(item, int) and item <= 5:
                    formatted_hotels += f"- Stars: {item}\n"
                elif ":" in str(item):  # Time slot typically contains colons
                    formatted_hotels += f"- Time Slot: {item}\n"
                else:
                    formatted_hotels += f"- Room Type: {item}\n"
            formatted_hotels += "\n"
        
        # Prepare customer info including name
        customer_info = f"Customer ID: {customer_id}, Name: {customer_name}"
        
        # Prepare the payload for the LLM
        completed_mail_prompt = prompt_template_mail.format(
            hotel_info=formatted_hotels,
            customer_info=customer_info
        )
        completed_sql_llm_query = create_sql_llm_query(
            prompt_template_SQL, 
            completed_mail_prompt, 
            system_prompt=system_prompt
        )
        
        # Generate the email for this customer
        result = cur.execute(completed_sql_llm_query).fetchall()
        print(result)
        response_str = result[0][0]
        
        # Parse the JSON inside the string
        response_json = json.loads(response_str)
        
        # Extract the email content
        email_content = response_json["choices"][0]["messages"]
        
        # Add to generated emails
        generated_mails.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'mail': email_content
        })
        
    return generated_mails



def main():
    prompt_template_target_customer = prompt_templates.prompt_target_customer
    prompt_template_recommand = prompt_templates.prompt_template_recommand
    prompt_template_mail = prompt_templates.prompt_template_mail
    prompt_template_SQL = prompt_templates.prompt_template_SQL
    system_prompt = prompt_templates.system_prompt
    prompt_template_SQL = prompt_templates.prompt_template_SQL

    # get the target customers
    query_exec_result_target_customer = get_target_customers(prompt_template_target_customer)

    # get the hotel recommendations
    list_ids_rec = get_hotel_recommendations(prompt_template_recommand, query_exec_result_target_customer)

    # generate the batch mail
    generated_mails = generate_batch_mail(list_ids_rec, prompt_template_mail, prompt_template_SQL, system_prompt)

    print(generated_mails)


if __name__ == "__main__":
    main()

