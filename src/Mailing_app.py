import os
import snowflake.connector
from dotenv import load_dotenv
import json
import requests
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
import utils.prompt_templates as prompt_templates
import openai
import datetime
import pathlib


load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

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




# -------------------------------------------- LLM part ------------------------------------------

def complete_prompts(prompt_template, sql_template, placeholder_value):
    # une fonction qui retourne une sql query constitué du prompt template complété par le placeholder et du system prompt
    formated_prompt = prompt_template.format(placeholder = placeholder_value)
    formated_prompt_sql = sql_template.format(placeholder = formated_prompt)
    
    return formated_prompt, formated_prompt_sql



def openai_llm_call(prompt: str):
    response = openai.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content


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
    
    # Check if there's an error in the API response
    if analyst_response_json.get('message') is None:
        error_msg = f"API Error: {analyst_response_json.get('error_code')} - Request ID: {analyst_response_json.get('request_id')}"
        print(error_msg)
        # Return empty DataFrame with error message as thought
        return [error_msg, "", "", pd.DataFrame()]
    
    content = analyst_response_json['message']['content']
    for item in content :
        if item['type'] == "sql":
            sql_query = item["statement"]
            sql_confidence = item["confidence"]
        elif item['type'] == "text":
            thought = item["text"]
    
    try:
        df = cur.execute(sql_query).fetch_pandas_all()
        query_exec_result = [thought, sql_query, sql_confidence, df]
        return query_exec_result
    except Exception as e:
        error_msg = f"SQL Error: {str(e)}"
        print(error_msg)
        return [thought, sql_query, sql_confidence, pd.DataFrame()]
    


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


def process_customer_ids(customer_df, prompt_template_reco):
    # gives hotel recommendation for each customer id
    # customer_df is a dataframe
    hotel_reco = []
    for i, row in customer_df.iterrows():
        customer_id = row['CUSTOMER_ID']
        
        # Format all customer information (excluding customer_id) as a string
        customer_info_dict = row.drop('CUSTOMER_ID').to_dict()
        customer_info = ', '.join([f"{key}: {value}" for key, value in customer_info_dict.items()])
        
        print(f"Processing customer {i}: {customer_id}")
        
        # create the payload for a customer id
        placeholder_reco = prompt_template_reco.format(customer_id=customer_id)
        payload_reco = create_user_payload(placeholder_reco)
        print(payload_reco)
        
        # get response: hotel to recommend
        analyst_response_reco = get_analyst_response(payload_reco)
        print(f"Analyst response for hotel recommendation for customer {i}: {customer_id}: \n {analyst_response_reco}")
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
            'customer_information': customer_info,
            'hotel_recommendation': query_exec_result_hotel_reco[3]
        })

    return hotel_reco





def get_target_customers(prompt_template_target_customer):
    # get the customer ids of customers that went in a hotel in Paris, France during the past 7 days
    # and the first name of the customer
    payload_target_customer = create_user_payload(prompt_template_target_customer)
    analyst_response_target_customer = get_analyst_response(payload_target_customer)
    query_exec_result_target_customer = get_query_exec_result(analyst_response_target_customer)
    print("---------- customer targets ----------")
    print(f"query_exec_result_target_customer entire result: \n{query_exec_result_target_customer}\n")
    print(f"df customer targets: \n{query_exec_result_target_customer[3]}\n")
    print(f"df customer targets type: \n{type(query_exec_result_target_customer[3])}\n")
    print("--------------------------------------")
    return query_exec_result_target_customer[3]



def get_hotel_recommendations(prompt_template_recommand, query_exec_result_target_customer):
    ###### to delete ???????
    """
    Args : 
    prompt_template_recommand : prompt telling how to retrieve the hotels to recommand to each customer and the informations to retrieve
    query_exec_result_target_customer : result of the query that retrieves the customer ids and the first names of the customers
    
    Returns : 
    list_ids_rec : list of dicts with customer ids and customer information and the hotel recommendations corresponding 
    """

    # get the hotel recommendations for the target customers
    df_customer_target = query_exec_result_target_customer[3]
    list_ids_rec = process_customer_ids(df_customer_target, prompt_template_recommand)

    return list_ids_rec



def generate_batch_mail(list_customer_hotel, prompt_template_mail, prompt_template_SQL, system_prompt, output_dir=None, snowflake_llm=True):
    # sql template mail is the sql query calling llm where we have to put the prompt 
    # prompt template mail is the template of the prompt sent to the llm where we have to add customer and hotel info
    # list_customer_hotel contains the customer info and hotel recommendations
    # snowflake_llm is a boolean to choose if we use snowflake llm or openai llm

    generated_mails = []
    for elt in list_customer_hotel:
        customer_id = elt["customer_id"]
        customer_information = elt["customer_information"]
        hotel_recommendations = elt["hotel_recommendation"]
        
        completed_mail_prompt = prompt_template_mail.format(
            hotel_info=hotel_recommendations,
            customer_info=customer_information
        )

        if snowflake_llm:
            # Generate the email for this customer with snowflake sql querying a llm
            completed_sql_llm_query = create_sql_llm_query(
                prompt_template_SQL, 
                completed_mail_prompt, 
                system_prompt=system_prompt
            )

            result = cur.execute(completed_sql_llm_query).fetch_pandas_all()
            print(f"result: \n{result}\n")
            response_str = result[0][0]
            
            # Parse the JSON inside the string
            response_json = json.loads(response_str)
            
            # Extract the email content
            email_content = response_json["choices"][0]["messages"]
        
        else:
            # Generate the email for this customer with openai llm
            email_content = openai_llm_call(completed_mail_prompt)


        # Add to generated emails
        mail_data = {
            'customer_id': customer_id,
            'customer_information': customer_information,
            'mail': email_content
        }
        
        generated_mails.append(mail_data)
        
        # Save email to file if output directory is provided
        if output_dir:
            # Create a filename based on customer info
            filename = f"{customer_id}_{customer_information.replace(' ', '_')}.json"
            filepath = os.path.join(output_dir, filename)
            
            # Save the email content as JSON
            with open(filepath, 'w') as f:
                json.dump(mail_data, f, indent=4)
            print(f"Email saved to {filepath}")
    
    return generated_mails


def create_email_output_directory():
    """Create a timestamped directory for email storage"""
    # Get current date and time
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create directory path
    dir_path = os.path.join("gen_mails", f"gen_mail_{timestamp}")
    
    # Create directory if it doesn't exist
    os.makedirs(dir_path, exist_ok=True)
    
    return dir_path


def main():
    prompt_template_target_customer = prompt_templates.prompt_target_customer
    prompt_template_recommand = prompt_templates.prompt_template_recommand
    prompt_template_mail = prompt_templates.prompt_template_mail
    prompt_template_SQL = prompt_templates.prompt_template_SQL
    system_prompt = prompt_templates.system_prompt
    prompt_template_SQL = prompt_templates.prompt_template_SQL

    # Create output directory for emails
    output_dir = create_email_output_directory()
    print(f"Emails will be saved to: {output_dir}")

    # Initialize timing metrics
    timing_metrics = {}

    # get the target customers
    start_time = datetime.datetime.now()
    query_exec_result_target_customer = get_target_customers(prompt_template_target_customer)
    end_time = datetime.datetime.now()
    timing_metrics['get_target_customers'] = (end_time - start_time).total_seconds()
    print(f"Time to get target customers: {timing_metrics['get_target_customers']} seconds")

    # get the hotel recommendations
    start_time = datetime.datetime.now()
    list_customer_hotel = process_customer_ids(query_exec_result_target_customer, prompt_template_recommand)
    print("---------- list_customer_hotel ----------")
    print(f"list_customer_hotel: \n{list_customer_hotel}\n")
    print("----------------------------------------")
    end_time = datetime.datetime.now()
    timing_metrics['get_hotel_recommendations'] = (end_time - start_time).total_seconds()
    print(f"Time to get hotel recommendations: {timing_metrics['get_hotel_recommendations']} seconds")

    # generate the batch mail and save to files
    start_time = datetime.datetime.now()
    generated_mails = generate_batch_mail(list_customer_hotel, prompt_template_mail, prompt_template_SQL, system_prompt, output_dir=output_dir, snowflake_llm=False)
    end_time = datetime.datetime.now()
    timing_metrics['generate_batch_mail'] = (end_time - start_time).total_seconds()
    print(f"Time to generate emails: {timing_metrics['generate_batch_mail']} seconds")
    
    # Add timing metrics to each file
    for mail_data in generated_mails:
        mail_data['timing_metrics'] = timing_metrics
        if output_dir:
            customer_id = mail_data['customer_id']
            customer_information = mail_data['customer_information']
            filename = f"{customer_id}_{customer_information.replace(' ', '_')}.json"
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(mail_data, f, indent=4)

    print(f"Generated {len(generated_mails)} emails. Saved to {output_dir}")
    print(f"Timing metrics: {timing_metrics}")


if __name__ == "__main__":
    main()

