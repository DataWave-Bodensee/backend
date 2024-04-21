from openai import OpenAI
import json
from webscraper.db_parameters import llm_tool, db_params


def llm_create_db_entry(article):
    """Takes an article as input and creates a new database entry, by utilizing a llm to extract the relevant information."""
    client = OpenAI()

    # Ask LLM, to call create_database_entry tool by extracting the relevant information from the given article.
    response = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant, aimed at analysing text data and extracting relevant information.",
            },
            {
                "role": "user",
                "content": "Make a new database entry for the following article:",
            },
            {"role": "user", "content": article.content},
        ],
        temperature = 1.0,
        tools = llm_tool,
        tool_choice = "auto",
    ) 
    response_message = response.choices[0].message
    tool_calls = response_message.tool_calls

    response_summary = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=[
            {
                "role": "system",
                "content": "You are a helpful assistant, aimed at analysing text data and extracting relevant information.",
            },
            {
                "role": "user",
                "content": "summarize the article in few sentences as possible:",
            },
            {"role": "user", "content": article.content},
        ],
        temperature=1.0,
    )

    response_message_summary = response_summary.choices[0].message.content

    if tool_calls:
        tool_call = tool_calls[0]  # LLM can possibly make multiple functions calls, only take the first

        if tool_call.function.name != "create_database_entry":
            print("Error: Unexpected function call.")
            return
        function_args = json.loads(tool_call.function.arguments)

        print(function_args["Relevant"])
        if function_args["Relevant"] == False:
            print("Article is not relevant to the database.")
            return

        # Check if all required arguments are present in the function call
        for arg in db_params:
            if arg not in function_args:
                print (f"Error: No {arg} found in the function call. Function call: {function_args}")
                return

        entry = {
            "title": article.title,
            "summary": response_message_summary, 
            "website": article.link,
            "content": article.content,
            "keywords": article.keywords,
            "date": function_args["Incident Date"],
            "number_dead": function_args["Number of Dead"],
            "number_missing": function_args["Number of Missing"],
            "number_survivors": function_args["Number of Survivors"],
            "country_of_origin": function_args["Country of Origin"],
            "region_of_origin": function_args["Region of Origin"],
            "cause_of_death": function_args["Cause of Death"],
            "region_of_incident": function_args["Region of Incident"],
            "country_of_incident": function_args["Country of Incident"],
            "location_of_incident": function_args["Location of Incident"],
            "latitude": function_args["Latitude"],
            "longitude": function_args["Longitude"]
        }

        return entry

    print("Error: No function call found.")
    return
