# Define a single tool for the LLM, namely create_database_entry, which takes all the necessary parameters to create a new entry in the database.
llm_tool = [{
    "type": "function",
    "function": {
        "name": "create_database_entry",
        "description": "Create a new entry in the database with the given parameters.",
        "parameters": {
            "type": "object",
            "properties": {
                "Region of Incident": {"type": "string", "description": ""},
                "Incident Date": {
                    "type": "string",
                    "description": "estimated date of death format: YYYY-MM-DD",
                },
                "Number of Dead": {
                    "type": "integer",
                    "description": "total number of dead people",
                },
                "Number of Missing": {
                    "type": "integer",
                    "description": "total number of those who are missing and are thus assumed to be dead",
                },
                "Number of Survivors": {
                    "type": "integer",
                    "description": "number of migrants that survived the incident",
                },
                "Country of Origin": {
                    "type": "string",
                    "description": "Country of birth of the decedent",
                },
                "Region of Origin": {
                    "type": "string",
                    "description": "Region of origin of the decedent(s)",
                },
                "Cause of Death": {
                    "type": "string",
                    "description": "determination of conditions resulting in the migrant's death i.e. the circumstances of the event that produced the fatal injury",
                },
                "Region of Incident": {
                    "type": "string",
                    "description": "Region where incident occured",
                },
                "Country of Incident": {
                    "type": "string",
                    "description": "country where incident occured",
                },
                "Location of Incident": {
                    "type": "string",
                    "description": "Place where the death(s) occurred or where the body or bodies were found. Nearby towns or cities or borders are included where possible. When incidents are reported in an unspecified location, this will be noted"
                },
                "Latitude":{
                    "type": "number",
                    "description": "Guess the latitude of the incident location"
                },
                "Longitude":{
                    "type": "number",
                    "description": "Guess the longitude of the incident location"
                },
                "Relevant": {
                    "type": "boolean",
                    "description": "We are gathering data about migrant deaths and getting missed. If the article is not relevant, set this to false.",
                },
            },
            "required": [
                "Region of Incident",
                "Incident Date",
                "Number of Dead",
                "Number of Missing",
                "Number of Survivors",
                "Country of Origin",
                "Region of Origin",
                "Cause of Death",
                "Country of Incident",
                "Location of Incident",
                "Latitude",
                "Longitude",
                "Relevant"
            ],
        },
    },
}]

db_params = [
                "Region of Incident",
                "Incident Date",
                "Number of Dead",
                "Number of Missing",
                "Number of Survivors",
                "Country of Origin",
                "Region of Origin",
                "Cause of Death",
                "Country of Incident",
                "Location of Incident",
                "Latitude",
                "Longitude"
            ]