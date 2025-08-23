import streamlit as st
import random
import time
import json
import re
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from langchain.graphs import Neo4jGraph
from langchain.vectorstores import Neo4jVector
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langgraph.graph import StateGraph, END
import json
from typing import TypedDict, Annotated, List, Dict
import operator
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AnyMessage, SystemMessage, HumanMessage, AIMessage, ChatMessage
from langchain.prompts import PromptTemplate
from IPython.display import Image
import logging
import os
from tqdm import tqdm
from pinecone import Pinecone, ServerlessSpec

load_dotenv()

NEO4J_URI=os.getenv("NEO4J_URI")
NEO4J_USERNAME=os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD=os.getenv("NEO4J_PASSWORD")
AURA_INSTANCEID=os.getenv("AURA_INSTANCEID")
AURA_INSTANCENAME=os.getenv("AURA_INSTANCENAME")

neo4j_graph = Neo4jGraph(url=NEO4J_URI, username=NEO4J_USERNAME, password=NEO4J_PASSWORD)
neo4j_schema = neo4j_graph.schema

OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")
PINECONE_API_KEY=os.getenv("PINECONE_API_KEY")

ENTITY_DEFINITION = """
Entity Definition:
DURATION: 
    - Numerical values that represent time duration.
    - Example: "5 days", "2 weeks"
START_DATE: 
    - Any format of dates, including natural language and seasonality which can be converted to standard format (YYYY-MM-DD).
    - Maps to: RestaurantReview.travel_date, AttractionReview.travel_date
    - Example: "October 10th", "First week Fall 2024"
END_DATE:
    - Any format of dates, including natural language and seasonality which can be converted to standard format (YYYY-MM-DD).
    - Maps to: RestaurantReview.travel_date, AttractionReview.travel_date
    - Example: "October 10th", "Last week of Fall 2024"
BUDGET:
    - Numerical values representing monetary amounts
    - Maps to attributes -> BUDGET: value < 100, MIDRANGE: 100 <= value <= 250, UPSCALE: 250 < value <= 300, LUXURY: value > 300
    - Example: "$500", "200-300", "budget_friendly"
LOCATION:
    - Name of any geographic location, like cities, countries, continents, districts, etc. Example: "Paris", "New York", "California", "San Francisco", "Seattle", "Los Santos"
    - Maps to: City.name, Attraction.city, Hotel.CITY, Restaurant.CITY, City.state, Attraction.STATE, Hotel.STATE, Restaurant.STATE
    - Example: "Paris", "New York", "California", "San Francisco"    
LOC_PREF:
    - Location, geographical or cultural preference such as mountain, beach, river, jungle, lake, west side, east side, temples, spiritual destination, adventure, leisure, luxury, etc.
    - Maps to: Category.name
    - Example: "beach", "mountain", "luxury", "adventure", "cultural landmarks"
FOOD_PREF:
    - Food preferences such as cuisine, dietary restrictions, etc.
    - Maps to: Cuisine.name, DietaryRestriction.name, Restaurant.CUSINES, Restaurant.DIETARY_RESTRICTIONS
    - Example: "Italian", "Vegan", "Gluten-free", "Spicy", "Halal", 'Indian'
TRAVEL_PREF:
    - Personal preferences for travel, such as solo, family, friends, group, couple, etc.
    - Maps to: TravelType.name
    - Example: "solo", "family", "friends", "couple"

Required Entity Nodes:
- DURATION
- START DATE
- END DATE
- BUDGET
- LOCATION

Note: Any two of DURATION, START DATE, and END DATE are required for a valid vacation plan.

Optional Entity Nodes:
- LOC_PREF
- FOOD_PREF
- TRAVEL_PREF
    
Following the given Knowledge Graph, the following relations are defined: The schema has Node and Relationship definitions as follows:
{neo4j_schema}

"""

SYSTEM_PROMPT_TEMPLATE_CASUAL_CONVERSATION = """
You are a travel agent assistant who carefully distinguishes between travel-related queries and casual conversation. Your responses must be in valid JSON format with exactly two fields: "travel" (boolean) and "response" (string).
 
Response Rules:
1. Travel Queries
   - Set "travel": true
   - Set "response": "" (empty string)
   - Examples: trip planning, destination questions, travel advice
2. Casual Conversation
   - Set "travel": false
   - Set "response": [single concise response]
   - Keep responses friendly but brief
   - Stay in character as a travel agent
 
Key Guidelines:
- Never include clarifying questions in responses
- Don't generate dialogue or multiple turns
- Always return syntactically valid JSON
- Maintain consistent response structure
 
Examples:
 
Travel-Related (return empty response):

Human: "What's the best time to visit Bali?"
Assistant:
{
    "travel": true,
    "response": ""
}

Casual Conversation:

Human: "How are you today?"
Assistant:
{
    "travel": false,
    "response": "I am going great, Thanks!! I'm here to help you plan your dream vacation!"
}
"""

SYSTEM_PROMPT_TEMPLATE_MISSING_INFORMATION = """

You are an Intelligent Entity Validator that helps to validate the missing information from the user input.

Following are the entities defined in the system:
{ENTITY_DEFINITION}

Check for each each entity in the user input twice and return the missing information in the response.

Follow the given format to find the missing information from the user input:

OUTPUT FORMAT:
{{"MISS": True | False, "response": "" | "<missing prompt here>"}}

MISSING INFORMATION EXAMPLE 1:
Human: "I have a budget of 1000$ for my vacation at Boston"
Assistant: {{"MISS": True, "response": "Your prompt has following missing information: "DURATION", "START_DATE", "END_DATE". Please provide the missing information."}}

MISSING INFORMATION EXAMPLE 2:
Human: "I have planned to go to Paris"
Assistant: {{"MISS": True, "response": "Your prompt has following missing information: DATE: Start Date, VAC_DUR: Vacation Duration, VAC_START: Vacation Start Date, VAC_END: Vacation End Date. Please provide the missing information."}}

NOT MISSING INFORMATION EXAMPLE:
Human: "I have a budget of 1000$ for my vacation at Boston, I have vaction duration of 5 days, I will start my vacation on 2022-12-25 and end on 2022-12-30"
Assistant: {{"MISS": False, "response": ""}}

Strict Note: JSON response should be in the given format only. and dont write string in the response.

"""

SYSTEM_PROMPT_TEMPLATE_TRIPLETS_EXTRACT = """
You are an Intelligent Triplets Extractor that helps to extract the triplets from the user input.
 
Strictly follow Entity Definition and Neo4j Schema to extract the entities and triplets from the user input.
{ENTITY_DEFINITION}
 
Note: Entities like DURATION, START_DATE, END_DATE are not present in the given schema, so add them in ENTITY but ignore them for KG extraction. KG extraction should be based on the given schema only.

Expected Output Format:
{{
  "ENTITY": {{
    "ENTITY_1": ["value"],
    "ENTITY_2": ["value"],
    ...
    }},
  "KG" : [{{
    "subject": {{
      "type": "NodeType",
      "properties": {{
        "attribute": ["values"]
      }}
    }},
    "relation": "RELATION_TYPE",
    "object": {{
      "type": "NodeType",
      "properties": {{
        "attribute": ["values"]
      }}
    }}
  }}]
}}


Rule 1: Logic for budget distribution:
If the budget is $1000:
Then 70 percent of the budget is allocated to the hotel and 30 percent is allocated to the restaurant.
Hence, Hotel budget - $700 and Restaurant budget - $300 for the total duration of the trip.
 
Rule 2: Following are different categories of each required node:
TierCluster: {TierCluster}
City: {City}
Category: {Category}
DietaryRestriction: {DietaryRestriction}
Cuisine: {Cuisine}
 
Match all extracted entities and attributes to the above categories (TierCluster, City, Category, DietaryRestriction, Cuisine) and rewrite the extracted entities and attributes to match the given categories.
Meaning, Change Category to nearest Category Node attributes from the Neo4j Category Node.
Example:
1. If extracted entity is ["California"] then change it to ["San Francisco"] which is the nearest city name from the Neo4j City Node (City.name).
2. If extracted entity is ["beach"] then change it to ["Nature & Parks", "Outdoor Activities"] which is the nearest category names from the Neo4j Category Node (Category.name, Attraction.primary_category attributes).
3. Convert all budget values to the nearest TierCluster Node values from the Neo4j TierCluster Node (TierCluster.tier, Hotel.HOTEL_TIER, Restaurant.RESTAURANT_TIER attributes) as per the given logic.
TierCluster Budget range Logic:
  - For Hotel:
      BUDGET: BUDGET < 100,  
      MIDRANGE: 100<= BUDGET < 200,
      UPSCALE: 200<= BUDGET < 300,
      LUXURY: 300<= BUDGET
  - For Restaurant:
      QUICK_SERVICE: BUDGET < 10,
      UPSCALE_CASUAL: 10<= BUDGET < 45,
      FINE_DINING: 45<= BUDGET
 
EXAMPLES:
 
1. Location & Budget:
Input: "I have a budget of 1000$ for my vacation in Boston"
Output: {{"ENTITY":{{
  "LOCATION": ["Boston"],
  "BUDGET": ["1000"]
  }},
  "KG":[
  {{
    "subject": {{
      "type": "City",
      "properties": {{
        "name": ["Boston"]
      }}
    }},
    "relation": "LOCATED_IN",
    "object": {{
      "type": "State",
      "properties": {{
        "name": ["Massachusetts"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Hotel",
      "properties": {{}}
    }},
    "relation": "LOCATED_IN",
    "object": {{
      "type": "City",
      "properties": {{
        "name": ["Boston"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Hotel",
      "properties": {{}}
    }},
    "relation": "IN_TIER_CLUSTER",
    "object": {{
      "type": "TierCluster",
      "properties": {{
        "tier": ["LUXURY"]
      }}
    }}
  }}
]
}}
 
2. Categories & Activities:
Input: "I want to visit beaches and cultural sites in Miami"
Output: {{
  "ENTITY":{{
  "LOCATION": ["Miami"],
  "LOC_PREF": ["beach", "cultural sites"]
}},
"KG": [
  {{
    "subject": {{
      "type": "City",
      "properties": {{
        "name": ["Miami"]
      }}
    }},
    "relation": "LOCATED_IN",
    "object": {{
      "type": "State",
      "properties": {{
        "name": ["Florida"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Attraction",
      "properties": {{}}
    }},
    "relation": "HAS_CATEGORY",
    "object": {{
      "type": "Category",
      "properties": {{
        "name": ["Nature & Parks", "Outdoor Activities"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Attraction",
      "properties": {{}}
    }},
    "relation": "HAS_CATEGORY",
    "object": {{
      "type": "Category",
      "properties": {{
        "name": ["Sights & Landmarks"]
      }}
    }}
  }}
]
}}
 
3. Food Preferences:
Input: "Looking for vegetarian Italian restaurants in New York"
Output: {{
  "ENTITY": {{
  "LOCATION": ["New York"],
  "FOOD_PREF": ["vegetarian", "Italian"]
}},
"KG": [
  {{
    "subject": {{
      "type": "Restaurant",
      "properties": {{}}
    }},
    "relation": "LOCATED_IN",
    "object": {{
      "type": "City",
      "properties": {{
        "name": ["New York"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Restaurant",
      "properties": {{}}
    }},
    "relation": "SERVES_CUISINE",
    "object": {{
      "type": "Cuisine",
      "properties": {{
        "name": ["Italian"]
      }}
    }}
  }},
  {{
    "subject": {{
      "type": "Restaurant",
      "properties": {{}}
    }},
    "relation": "ACCOMMODATES",
    "object": {{
      "type": "DietaryRestriction",
      "properties": {{
        "name": ["vegetarian"]
      }}
    }}
  }}
]
}}
 
VALIDATION RULES:
1. Every location must have a valid City-State relationship
2. Categories must match existing Category nodes
3. Price ranges must map to valid PriceCluster categories
4. All attributes must be in list format, even for single values
5. Relationships must match the Neo4j schema exactly

Additionally, try to create more advanced nested triplets and validate the Neo4j schema with the extracted triplets.
 
Strict Note: Format should be json output with the given format only, no string should be written in the response.
"""

SYSTEM_PROMPT_TEMPLATE_ITERNARY = """
You are an Intelligent Itinerary Planner that helps to create a vacation plan for the user.

Following are the retrieved data related to Restaurants, Hotels, and Attractions from Knowledge Graph:
{KG_DATA}

Following are the retrieved data related to Restaurants, Hotels, and Attractions from Vector Store:
{KG_DATA}

Use both the data sources to create a detailed vacation plan for the user which best suits the user's preferences, budget and ratings.
Based on User's preferences, you need to create a vacation plan for the user.

Provide a detailed date wise plan for the user's vacation including the following:
- Hotel stay (with check-in and check-out dates) and ratings Give two hotel options based on the budget.
- Attractions to visit on each day
- Restaurants to dine at each day - Include cuisine type and price range
- Total budget spent on each day

Output:
Plan: Detailed vacation plan here
Data: Metadata of Hotels, Attractions, Restaurants linke links, longitudes, longitudes, names, ratings, cuisines, price ranges, etc. which are used to create the vacation plan.

Strict Note: JSON response should be in the given format only. and dont write string in the response.


Ouput Format:
{{
    "Plan": "Detailed vacation plan here",
    "Data": {{
        "Hotel": {{
            "Hotel Name": ["name here","name here"]
            "Links": ["links here", "links here"]
            "Longitudes": ["longitudes here", "longitudes here"]
            "Latitudes": ["latitudes here", "latitudes here"]
        }},
        "Attractions": {{
            "Attraction Name": ["name here","name here"]
            "Links": ["links here", "links here"]
            "Longitudes": ["longitudes here", "longitudes here"]
            "Latitudes": ["latitudes here", "latitudes here"]
        }},
        "Restaurants": {{
            "Restaurant Name": ["name here","name here"]
            "Links": ["links here", "links here"]
            "Longitudes": ["longitudes here", "longitudes here"]
            "Latitudes": ["latitudes here", "latitudes here"]
        }}
    }}
}}

"""

ENTITY_REFINEMENT_VECTOR_TEMPLATE = """
You are an Intelligent Entity Refiner that helps refine extracted entities from the user input.

The List of Entities from the Knowledge Graph that should be used to refine the extracted entities:
{CATEGORY_ENTITIES}

Strictly follow category attribute types to refine the extracted entities from the user input.  
Convert all extracted entities to the nearest category node attributes from the Neo4j knowledge graph nodes (e.g., `Category.name`, `Attraction.primary_category` attributes).  

Examples of refinement logic:  
- Convert location entities (e.g., ["California"]) to the nearest city name from the Neo4j City Node (e.g., `City.name`).  
  Example: ["California"] → ["San Francisco"].  
- Convert location preferences (e.g., ["beach"]) to the nearest category names from the Neo4j Category Node (e.g., `Category.name`, `Attraction.primary_category` attributes).  
  Example: ["beach"] → ["Nature & Parks", "Outdoor Activities"].  
- Convert budget values to the nearest TierCluster Node values from the Neo4j TierCluster Node (e.g., `TierCluster.tier`, `Hotel.HOTEL_TIER`, `Restaurant.RESTAURANT_TIER` attributes) as per the provided logic.  
  Example: ["500"] → ["MIDRANGE"].  
- Convert food preferences to match the cuisine most associated with the city.  
  Example: ["Cultural"] → ["Italian"], because San Francisco is famous for Italian cuisine.  
- Convert travel preferences to standardized values.  
  Example: ["alone"] → ["Solo"].  
- Map dietary restrictions to relevant categories.  
  Example: ["vegetarian"] → ["Vegetarian friendly"], ["vegan"] → ["Vegan options"].  

After refining the entities, generate filter queries for vector search in Pinecone.  
Key attributes for the `attractions`, `hotels`, and `restaurants` namespaces:  
- `Attraction`: ["CITY", "STATE", "PRIMARY_CATEGORY"] // use attribute only if it exists in the extracted entities  
- `Hotel`: ["CITY", "PRICE_RANGE", "RATING"]  // use attribute only if it exists in the extracted entities  
- `Restaurant`: ["CITY", "DIETARY_RESTRICTIONS", "RATING", "CUISINES"] // use attribute only if it exists in the extracted entities  

### Expected Output Format should be json format with the given format only, no string should be written in the response.
{{
  "attractions": {{"CITY": {{"$eq": "San Francisco"}}, "PRIMARY_CATEGORY": {{"$eq": "Nature & Parks", "$eq": "Outdoor Activities"}}, "PRICE_RANGE": {{"$eq": "MIDRANGE"}}, "RATING": {{"$eq": 5}}}},
  "restaurants": {{"CITY": {{"$eq": "San Francisco"}}, "CUISINES": {{"$eq": "Italian"}}, "RATING": {{"$eq": 5}}}},
  "restaurants": {{"CITY": {{"$eq": "San Francisco"}}, "DIETARY_RESTRICTIONS": {{"$eq": "Vegetarian friendly", "$eq": "Vegan options"}}, "RATING": {{"$eq": 5}}}}
}}
"""


log_file_path = os.path.join(os.getcwd(), 'travel_agent.log')
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info("*"*50 + "Travel Agent Log" + "*"*50)
logging.info("Logging has started.")



entity = PromptTemplate(template=ENTITY_DEFINITION, input_variables=["neo4j_schema"])
entity_prompt = entity.format(neo4j_schema=neo4j_schema)


def get_human_prompt_template_conversation(system_prompt, human_prompt):
    message = [
        system_prompt,
        HumanMessage(human_prompt)
    ]
    return message


class OpenAIModel:
    def __init__(self, model_name="gpt-4o-mini"):
        self.model = ChatOpenAI(model=model_name)
        self.status = None
        
    def replace_single_quotes(self, input_string):
        """
        Replaces single quotes not used in contractions with double quotes.
        """
        # Define a regex pattern to match contractions with apostrophes (e.g., I'm, I've, You're)
        contraction_pattern = r"\b\w+'(?:[a-zA-Z]+|d|ve|ll|re|s|t)\b"
        
        input_string = input_string.replace("\"", "'")

        # Temporarily replace contractions to protect them during processing
        protected_string = re.sub(contraction_pattern, lambda m: m.group(0).replace("'", "\uFFFF"), input_string)

        # Replace remaining single quotes with double quotes
        replaced_string = protected_string.replace("'", "\"")

        # Restore the protected contractions
        final_string = replaced_string.replace("\uFFFF", "'")

        return final_string
        
    def generate_json(self, messages):
        response = self.model.generate([messages])
        response = response.generations[0][0].text
        
        try:
            response = self.replace_single_quotes(response)
            response = response.replace("True", "true")
            response = response.replace("False", "false")
            response = response.replace("None", "null")
            response = json.loads(response)
            self.status = "json loaded successfully for response"
        except Exception as e:
            response = response.strip()
            self.status = str(e)
        return response
    
    def generate_text(self, messages):
        response = self.model.generate([messages])
        response = response.generations[0][0].text
        return response
    
    def get_status(self):
        return self.status

class AgentState(TypedDict):
    user_prompt: str
    entity: str | Dict[str, List[str]]
    plan: str
    err: str
    phase_1: str
    phase_2: str
    triplets: str
    refine_triplets: str
    response: str
    retrieve_data_kg: List[List[str]]
    retrieve_data_vector: List[Dict[str, str | Dict[str, str]]]
    cypher_queries: List[str]
    iternary_generated: str
    category_entity: Dict[str, List[str]]

class TravelAgent:
    def __init__(self, model):
        self.graph = None
        self.memory = MemorySaver()
        self.status = None
        self.builder = None
        self.model = model
        
    def casual_extract(self, state: AgentState):
        # system_msg = PromptTemplate(template=SYSTEM_PROMPT_TEMPLATE_CASUAL_CONVERSATION)
        system_msg_str = SystemMessage(SYSTEM_PROMPT_TEMPLATE_CASUAL_CONVERSATION)
        human_msg = state["user_prompt"]
        messages = get_human_prompt_template_conversation(system_msg_str, human_msg)
        response = self.model.generate_json(messages)
        logging.info(f"Response: {response}")
        return {"phase_1": response, "response": response["response"]}
    
        
    def replace_single_quotes(self, input_string):
        """
        Replaces single quotes not used in contractions with double quotes.
        """
        # Define a regex pattern to match contractions with apostrophes (e.g., I'm, I've, You're)
        contraction_pattern = r"\b\w+'(?:[a-zA-Z]+|d|ve|ll|re|s|t)\b"
        
        input_string = input_string.replace("\"", "'")

        # Temporarily replace contractions to protect them during processing
        protected_string = re.sub(contraction_pattern, lambda m: m.group(0).replace("'", "\uFFFF"), input_string)

        # Replace remaining single quotes with double quotes
        replaced_string = protected_string.replace("'", "\"")

        # Restore the protected contractions
        final_string = replaced_string.replace("\uFFFF", "'")

        return final_string
        
    def missing_entity(self, state: AgentState):
        system_msg = PromptTemplate(template=SYSTEM_PROMPT_TEMPLATE_MISSING_INFORMATION, input_variables=["ENTITY_DEFINITION"])
        system_msg_str = SystemMessage(system_msg.format(ENTITY_DEFINITION=entity_prompt))
        human_msg = state["user_prompt"]
        messages = get_human_prompt_template_conversation(system_msg_str, human_msg)
        response = self.model.generate_json(messages)
        return {"phase_2": response, "response": response["response"]}
    
    def triplet_gen(self, state: AgentState):
        
        category = neo4j_graph.query("MATCH (c:Category) RETURN c.name as Category")
        city = neo4j_graph.query("MATCH (c:City) RETURN c.name as City")
        food_pref = neo4j_graph.query("MATCH (c:Cuisine) RETURN c.name as Cuisine")
        diet_pref = neo4j_graph.query("MATCH (c:DietaryRestriction) RETURN c.name as DietaryRestriction")
        tiercluster = neo4j_graph.query("MATCH (c:TierCluster) RETURN c.tier as TierCluster")
        
        # convert into list
        category = list(map(operator.itemgetter("Category"), category))
        city = list(map(operator.itemgetter("City"), city))
        food_pref = list(map(operator.itemgetter("Cuisine"), food_pref))
        diet_pref = list(map(operator.itemgetter("DietaryRestriction"), diet_pref))
        tiercluster = list(map(operator.itemgetter("TierCluster"), tiercluster))
        
        category_entities = {
            "Category": category,
            "City": city,
            "Cuisine": food_pref,
            "DietaryRestriction": diet_pref,
            "TierCluster": tiercluster
        }
        
        system_msg = PromptTemplate(template=SYSTEM_PROMPT_TEMPLATE_TRIPLETS_EXTRACT, input_variables=["ENTITY_DEFINITION", "TierCluster", "City", "Category", "Cuisine", "DietaryRestriction"])
        system_msg_str = SystemMessage(system_msg.format(ENTITY_DEFINITION=entity_prompt, TierCluster=tiercluster, City=city, Category=category, Cuisine=food_pref, DietaryRestriction=diet_pref))
        human_msg = state["user_prompt"]
        messages = get_human_prompt_template_conversation(system_msg_str, human_msg)
        # response = llms_model.invoke(messages)
        response = self.model.generate_json(messages)

        return {"triplets": response, "category_entity": category_entities, "response": "Triplets extracted successfully."}
    
    def generate_advanced_cypher_queries(self, triplets, property_config=None, max_results=10):
        """
        Generates optimized Cypher queries with advanced relationship handling and property filtering.
        Args:
            triplets: List of relationship triplets from knowledge graph
            property_config: Dictionary specifying which properties to return for each node type (optional)
            max_results: Maximum number of results to return per query (default: 10)
        Returns:
            List of optimized Cypher queries with focused property selection
        """

        cypher_queries = []
        # Track entities and their relationships for advanced query building
        entity_tracker = {
            'City': {'properties': set(), 'relationships': set()},
            'Hotel': {'properties': set(), 'relationships': set()},
            'Restaurant': {'properties': set(), 'relationships': set()},
            'Attraction': {'properties': set(), 'relationships': set()}
        }

        def build_return_clause(node_alias, node_type):
            """Helper function to build RETURN clause with specific properties"""
            if node_type not in property_config:
                return f"{node_alias}"
            props = property_config[node_type]
            return ", ".join(f"{node_alias}.{prop} as {node_type}_{prop}" for prop in props)

        # First pass: Process basic relationship queries
        for triplet in triplets:
            subject = triplet.get("subject", {})
            relation = triplet.get("relation", "")
            obj = triplet.get("object", {})

            subject_type = subject.get("type", "")
            subject_props = subject.get("properties", {})
            obj_type = obj.get("type", "")
            obj_props = obj.get("properties", {})

            # Track entity usage for advanced query generation
            if subject_type in entity_tracker:
                # Convert list values in properties to tuples to make them hashable
                for k, v in subject_props.items():
                    entity_tracker[subject_type]['properties'].add((k, tuple(v)))
                if relation:
                    entity_tracker[subject_type]['relationships'].add(relation)

            if relation:
                # Build base query with property filtering
                query = f"""
                MATCH (n:{subject_type})-[r:{relation}]->(m:{obj_type})
                """
                conditions = []
                # Add property conditions with type safety
                if subject_props:
                    conditions.extend(
                        f"n.{k} = {repr(v[0])}" 
                        for k, v in subject_props.items()
                    )
                if obj_props:
                    conditions.extend(
                        f"m.{k} = {repr(v[0])}" 
                        for k, v in obj_props.items()
                    )

                if conditions:
                    query += f"\nWHERE {' AND '.join(conditions)}"

                # Build RETURN clause with specified properties
                return_items = []
                return_items.append(build_return_clause("n", subject_type))
                return_items.append("type(r) as relationship_type")
                return_items.append(build_return_clause("m", obj_type))

                query += f"\nRETURN {', '.join(filter(None, return_items))}"
                # Add intelligent ordering based on node type
                query += """
                ORDER BY
                    CASE 
                        WHEN n:Hotel THEN coalesce(n.rating, 0)
                        WHEN n:Restaurant THEN coalesce(n.rating, 0)
                        WHEN n:Attraction THEN coalesce(n.rating, 0)
                        ELSE 0 
                    END DESC
                """
                query += f"\nLIMIT {max_results}"
                cypher_queries.append(query)

        # Additional specialized queries if needed
        return cypher_queries

    
    def cypher_gererator(self, state: AgentState):
        
        custom_props = {

            'Hotel': ['name', 'rating', 'price_range'],

            'Restaurant': ['name', 'cuisine_list'],

            'Attraction': ['name', 'primary_category']

        }

        # Default property configuration if none provided
        # if property_config is None:
        property_config = {
                'Hotel': ['name', 'rating', 'price_range', 'latitude', 'longitude', 'city', 'address'],
                'Restaurant': ['name', 'rating', 'price_category', 'primary_cuisine', 'latitude', 'longitude', 'city', 'address'],
                'Attraction': ['name', 'rating', 'primary_category', 'latitude', 'longitude', 'city', 'address'],
                'City': ['name', 'state']
            }

        # Generate queries with custom property selection

        queries = self.generate_advanced_cypher_queries(
            triplets=state["triplets"]["KG"],
            property_config=property_config,
            max_results=5
        )

        
        data = []
        for query in queries:
            da = neo4j_graph.query(query)
            if da:
                data.append(da)
                
        return {"retrieve_data_kg": data, "cypher_queries": queries, "response": "Cypher queries generated successfully."}
    
    def get_vector_search_queries(self, state: AgentState):
        # Prepare the system message and extract the response
        system_msg = PromptTemplate(template=ENTITY_REFINEMENT_VECTOR_TEMPLATE, input_variables=["CATEGORY_ENTITIES"])
        system_msg_str = SystemMessage(system_msg.format(CATEGORY_ENTITIES=state["category_entity"]))
        human_msg = state["user_prompt"]
        messages = get_human_prompt_template_conversation(system_msg_str, human_msg)
        response = self.model.generate_json(messages)

        # Extract search parameters with defaults
        city = response.get("attractions", {}).get("CITY", {}).get("$eq", "")
        primary_category = response.get("attractions", {}).get("PRIMARY_CATEGORY", {}).get("$eq", "")
        cuisine = response.get("restaurants", {}).get("CUISINES", {}).get("$eq", [])
        dietary = response.get("restaurants", {}).get("DIETARY_RESTRICTIONS", {}).get("$eq", [])
        
        logging.info(f"City: {city}, Primary Category: {primary_category}, Cuisine: {cuisine}, Dietary: {dietary}")
        logging.info(f"Refined Response: {response}")

        # Initialize Pinecone and the embeddings model
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index_name = "exp1"

        # Ensure the Pinecone index is ready
        existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]
        if index_name not in existing_indexes:
            pc.create_index(
                name=index_name,
                dimension=3072,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
            while not pc.describe_index(index_name).status["ready"]:
                time.sleep(1)

        index = pc.Index(index_name)
        embeddings = OpenAIEmbeddings(model="text-embedding-ada-002")

        # Query Pinecone for attractions
        attractions_filter = {"CITY": {"$eq": city}}
        if primary_category:
            attractions_filter["PRIMARY_CATEGORY"] = {"$eq": primary_category}

        response_vectors_attractions = index.query(
            namespace="attractions",
            vector=embeddings.embed_query(state["user_prompt"]),
            top_k=5,
            include_values=True,
            include_metadata=True,
            filter=attractions_filter,
        )

        if not response_vectors_attractions.get("matches"):
            response_vectors_attractions = index.query(
                namespace="attractions",
                vector=embeddings.embed_query(city),
                top_k=5,
                include_values=True,
                include_metadata=True,
                filter={"CITY": {"$eq": city}},
            )

        # Query Pinecone for hotels
        response_vectors_hotels = index.query(
            namespace="hotels",
            vector=embeddings.embed_query(state["user_prompt"]),
            top_k=5,
            include_values=True,
            include_metadata=True,
            filter={"CITY": {"$eq": city}},
        )
        
        if not response_vectors_hotels.get("matches"):
            response_vectors_hotels = index.query(
                namespace="hotels",
                vector=embeddings.embed_query(city),
                top_k=5,
                include_values=True,
                include_metadata=True,
                filter={"CITY": {"$eq": city}},
            )

        # Query Pinecone for restaurants
        restaurant_filter = {"CITY": {"$eq": city}}
        if cuisine:
            restaurant_filter["CUISINES"] = {"$eq": cuisine}
        if dietary:
            restaurant_filter["DIETARY_RESTRICTIONS"] = {"$eq": dietary}

        response_vectors_restaurants = index.query(
            namespace="restaurants",
            vector=embeddings.embed_query(state["user_prompt"]),
            top_k=5,
            include_values=True,
            include_metadata=True,
            filter=restaurant_filter,
        )
        
        if not response_vectors_restaurants.get("matches"):
            response_vectors_restaurants = index.query(
                namespace="restaurants",
                vector=embeddings.embed_query(city),
                top_k=5,
                include_values=True,
                include_metadata=True,
                filter={"CITY": {"$eq": city}},
            )

        # Serialize results for consistent output
        def serialize_pinecone_response(response):
            return [
                {
                    "id": match.get("id"),
                    "score": match.get("score"),
                    "metadata": match.get("metadata", {})
                }
                for match in response.get("matches", [])
            ]

        retrieve_data_vector = {
            "attractions": serialize_pinecone_response(response_vectors_attractions),
            "hotels": serialize_pinecone_response(response_vectors_hotels),
            "restaurants": serialize_pinecone_response(response_vectors_restaurants),
        }

        return {
            "retrieve_data_vector": retrieve_data_vector,
            "response": "Vector search queries generated successfully.",
        }

    
    def create_iternary(self, state: AgentState):
        system_msg = PromptTemplate(template=SYSTEM_PROMPT_TEMPLATE_ITERNARY, input_variables=["KG_DATA", "VECTOR_DATA"])
        system_msg_str = SystemMessage(system_msg.format(KG_DATA=state["retrieve_data_kg"], VECTOR_DATA=state["retrieve_data_vector"]))
        human_msg = state["user_prompt"]
        messages = get_human_prompt_template_conversation(system_msg_str, human_msg)
        logging.info(f"Messages: {messages}")
        response = self.model.generate_text(messages)
        logging.info(f"Response: {response}")
        return {"iternary_generated": response, "response": "Iternary created successfully."}
    
    def is_casual(self, state: AgentState):
        entity = state["phase_1"]
        logging.info(f"Evaluating if the user prompt is casual: {entity}")
        try:
            if type(entity) == str:
                entity = self.replace_single_quotes(entity)
                entity = entity.replace("True", "true")
                entity = entity.replace("False", "false")
        
                entity = entity.replace("None", "null")
                logging.info(f"User prompt after replacing single quotes: {entity}")
                entity = json.loads(entity)
            elif type(entity) == dict:
                pass
            logging.info(f"Router is_casual: {entity['travel']}")
            return entity["travel"]
        except Exception as e:
            state["err"] = str(e)
            logging.error(f"Error in is_casual: {str(e)}")
            
    def is_missing(self, state: AgentState):
        entity = state["phase_2"]
        logging.info(f"Evaluating if the user prompt is casual: {entity}")
        try:
            if type(entity) == str:
                entity = self.replace_single_quotes(entity)
                entity = entity.replace("True", "true")
                entity = entity.replace("False", "false")
                entity = entity.replace("None", "null")
                entity = json.loads(entity)
            elif type(entity) == dict:
                entity = entity
            
            logging.info(f"Router is_missing: {entity['MISS']}")
            return entity["MISS"]
        except Exception as e:
            state["err"] = str(e)
            logging.error(f"Error in is_missing: {str(e)}")

            
    def graph_builder(self):
        builder = StateGraph(AgentState)
        builder.add_node("casual_check", self.casual_extract) 
        builder.add_node("missing_entity", self.missing_entity)
        builder.add_node("triplet_formation", self.triplet_gen)
        # builder.add_node("triplet_refine", self.triplet_refine)
        builder.add_node("cypher_query_generator", self.cypher_gererator)
        builder.add_node("vector_search_queries", self.get_vector_search_queries)
        builder.add_node("iternary", self.create_iternary)

        builder.add_conditional_edges("casual_check", self.is_casual, {True: "missing_entity", False: END})
        builder.add_conditional_edges("missing_entity", self.is_missing, {True: END, False: "triplet_formation"})
        builder.add_edge("casual_check", END)
        # builder.add_edge("triplet_formation", "triplet_refine")
        builder.add_edge("triplet_formation", "cypher_query_generator")
        builder.add_edge("cypher_query_generator", "vector_search_queries")
        builder.add_edge("vector_search_queries", "iternary")
        builder.add_edge("iternary", END)
        # builder.add_edge("triplet_formation", END)
        
        builder.set_entry_point("casual_check")
        
        self.graph = builder.compile(checkpointer=self.memory)
        
    def plot_graph(self):
        display(Image(self.graph.get_graph().draw_mermaid_png()))
	
    def run(self, user_prompt, thread_id):
          thread = {
              "configurable": {
                  "thread_id": thread_id
              }
          }

          progress_steps = [
          "Checking missing entities...",
          "Generating Cypher queries...",
          "Performing Vector search..",
          "Fetching data from the database...",
          "Creating the itinerary...",
          "Finalizing itinerary.."
      ]
          # Placeholder for progress updates
          progress_placeholder = st.empty()
          progress_bar = st.progress(0)  # Initialize progress bar

          # Total number of progress steps (arbitrary or based on experience)
          total_steps = 6

          # Step counter
          current_step = 0

          # Call the `run` function and iterate over the generator
          for s in agent.graph.stream({'user_prompt': user_prompt}, {"configurable": {"thread_id": thread_id}}):
              current_step += 1

              # Update progress UI
              progress_placeholder.markdown(f"**Step {current_step}/{total_steps}: {progress_steps[current_step-1]}**")
              progress_bar.progress(current_step / total_steps)

              # Simulate time for each step (optional, based on actual time taken by `stream`)
              time.sleep(1)  # Adjust based on how fast `graph.stream` yields

              # Break the loop when progress reaches total steps (to avoid infinite updates)
              if current_step >= total_steps:
                  break

          # Clear progress UI after completion
          progress_placeholder.empty()
          progress_bar.empty()

          # for s in self.graph.stream(
          #     {'user_prompt': user_prompt},
          #     thread
          # ):
          print(s)
          
          return self.graph.get_state(thread)[0], self.graph.get_state(thread)[0]["response"]


# Initialize the Streamlit app
st.title("Travel Recommendation Chatbot")
st.markdown("Welcome! I'm your travel assistant. Ask me anything about planning your trip!")

# Instantiate the TravelAgent with the desired model (replace `YourModelClass` with the actual class)
open_ai = OpenAIModel()
agent = TravelAgent(model=open_ai)
agent.graph_builder()

# Initialize message history
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! How can I assist you with your travel plans today?"}
    ]

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if user_prompt := st.chat_input("Type your travel question here..."):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": user_prompt})
    with st.chat_message("user"):
        st.markdown(user_prompt)

    try:
      thread_id = "thread_1"  # Unique identifier for the thread
      state, response = agent.run(user_prompt, thread_id)

      # Add assistant's response to chat history
      st.session_state.messages.append({"role": "assistant", "content": response})

      # Display assistant response
      with st.chat_message("assistant"):
          st.success("Recommendations Generated Successfully!")
          st.markdown(response, unsafe_allow_html=True)  # Display response as Markdown

          # Parse the itinerary data from the string into a dictionary
          if isinstance(state, dict) and "iternary_generated" in state:
              itinerary_data_str = state["iternary_generated"]
              
              # Parse the string into a dictionary
              try:
                  itinerary_data = json.loads(itinerary_data_str)
              except json.JSONDecodeError:
                  st.error("Failed to decode the itinerary data.")
                  itinerary_data = None

              if itinerary_data and isinstance(itinerary_data, dict):
                  # Display the plan
                  st.subheader("Itinerary Plan:")
                  st.markdown(itinerary_data.get("Plan", "No itinerary details available."), unsafe_allow_html=True)

                  # Hotel details
                  hotel_data = itinerary_data.get("Data", {}).get("Hotel", {})
                  if isinstance(hotel_data, dict) and hotel_data.get("Links"):
                      st.subheader("Hotel Details")
                      for i, link in enumerate(hotel_data.get("Links", [])):
                          hotel_name = hotel_data.get("Hotel Name", ["Unknown"])[i]  # Access hotel names correctly
                          st.markdown(f"**Hotel Name {i+1}:** {hotel_name}")
                          st.markdown(f"**Hotel Link {i+1}:** [Visit Hotel]({link})")
                          st.markdown(
                              f"**Location:** Latitude {hotel_data['Latitudes'][i]}, Longitude {hotel_data['Longitudes'][i]}"
                          )
                  else:
                      st.info("No hotel details available.")

                  # Attractions details
                  attraction_data = itinerary_data.get("Data", {}).get("Attractions", {})
                  if isinstance(attraction_data, dict) and attraction_data.get("Links"):
                      st.subheader("Attractions")
                      for i, link in enumerate(attraction_data.get("Links", [])):
                          attraction_name = attraction_data.get("Attraction Name", ["Unknown"])[i]  # Access attraction names correctly
                          st.markdown(f"**Attraction Name {i+1}:** {attraction_name}")
                          st.markdown(f"**Attraction Link {i+1}:** [Learn More]({link})")
                          st.markdown(
                              f"**Location:** Latitude {attraction_data['Latitudes'][i]}, Longitude {attraction_data['Longitudes'][i]}"
                          )
                  else:
                      st.info("No attractions details available.")

                  # Restaurant details
                  restaurant_data = itinerary_data.get("Data", {}).get("Restaurants", {})
                  if isinstance(restaurant_data, dict) and restaurant_data.get("Links"):
                      st.subheader("Restaurants")
                      for i, link in enumerate(restaurant_data.get("Links", [])):
                          restaurant_name = restaurant_data.get("Restaurant Name", ["Unknown"])[i]  # Access restaurant names correctly
                          st.markdown(f"**Restaurant Name {i+1}:** {restaurant_name}")
                          st.markdown(f"**Restaurant Link {i+1}:** [Explore]({link})")
                          st.markdown(
                              f"**Location:** Latitude {restaurant_data['Latitudes'][i]}, Longitude {restaurant_data['Longitudes'][i]}"
                          )
                  else:
                      st.info("No restaurant details available.")
              else:
                  st.error("Itinerary data is improperly formatted.")
          else:
              st.error("Itinerary data not found in the state.")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
                  


