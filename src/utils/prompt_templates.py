# prompt templates
prompt_template_mail = """
Write an e-mail, recommanding these hotels:
HOTEL INFORMATION (use all the informations provided):
{hotel_info}

to the person whose information are precised here:
CUSTOMER INFORMATION (use all the informations provided):
{customer_info}.

Additionnal instructions : 
- You should only rely on the informations that you explicitelly know from the HOTEL INFORMATION. 
- You should only rely on the information present in CUSTOMER INFORMATION. Do not hallucinate data that is not in those sections.
- I want you to call the customer by its first name, and to use the language of the customer to write the email.
- You should use all the informations provided in the CUSTOMER INFORMATION and the HOTEL INFORMATION to write the email.
- If parts of the email are not in the customer's language, translate them to the customer's language.
"""

prompt_target_customer = """Give me 2 customer_ids of customers that went in a hotel in Paris, France during the past 7 days.
For each customer ID, also give 
- the first name of the customer,
- its language.
"""

# May use a ML model for the recommandatioin later instead of this SQL query
prompt_template_recommand = """Give me only 2 hotels to recommand to this customer : {customer_id}. 
The 2 hotels have to be available from tomorrow untill 7 days after tomorrow and for a timeslot during which the client has already historically booked an offer.
For each hotel selected, give the subsequent informations : 
- hotel name, 
- time slot, 
- type of room,
- date of the booking,
- link to the offer.
"""

# __________________________

prompt_template_SQL = """
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama2-70b-chat',
        [
            {{'role': 'system', 'content': 'You are a helpful AI assistant that is writing some emails.' }},
            {{'role': 'user', 'content': '{placeholder}' }}
        ], {{}}
    ) as response;
"""

# prompt for the LLM to write the emails
system_prompt = """
You are a helpful AI assistant that is writing some emails.
"""