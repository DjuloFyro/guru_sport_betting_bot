from langchain.document_loaders import AsyncChromiumLoader
from langchain.document_transformers import BeautifulSoupTransformer
import pprint
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import create_extraction_chain
from langchain.chat_models import ChatOpenAI
import os
import pandas as pd


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, temperature=0, model="gpt-3.5-turbo-0613")

schema = {
    "properties": {
        "table_title": {"type": "string"},
        "table": {"type": "array", "items": {"type": "array", "items": {"type": "string"}}},
    },
    "required": ["table_title", "table"],
}

schema_equipe_result = {
    "properties": {
        "team_name": {"type": "string"},
        "table": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "DATE": {"type": "string"},
                    "MATCH": {"type": "string"},
                    "RESULT": {"type": "string"},
                    "COMPETITION": {"type": "string"}
                },
                "required": ["DATE", "MATCH", "RESULT", "COMPETITION"]
            }
        }
    },
    "required": ["table_title", "table"]
}

schema_schedule = {
    "properties": {
        "current_date": {"type": "string"},
        "matches": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "hour": {"type": "string"},
                    "first_team_name": {"type": "string"},
                    "second_team_name": {"type": "string"},
                },
                "required": ["hour", "first_team_name", "second_team_name"]
            }
        }
    },
    "required": ["current_date", "matches"]
}
schema_2 = {
    "properties": {
        "current_date": {"type": "string"},
        "matches": {
            "type": "array",
            "items": {
                "properties": {
                    "score_board_score_home_team": {"type": "string"},
                    "score_board_score_away_team": {"type": "string"},
                    "time": {"type": "string"},
                    "home_team_name": {"type": "string"},
                    "home_team_record": {"type": "string"},
                    "away_team": {"type": "string"},
                    "away_team_record": {"type": "string"},
                    "stadium": {"type": "string"},
                    "location": {"type": "string"},
                    "line": {"type": "string"},
                    "over_under": {"type": "string"},
                },
                "required": [
                    "home_team",
                    "home_team_record",
                    "away_team",
                    "away_team_record",
                    "stadium",
                ],
            },
        },
    },
    "required": ["date", "matches"],
}


schema_team_squad = {
    "properties": {
        "squad_name": {"type": "string"},
        "goalkeepers": {
            "type": "array",
            "items": {
                "properties": {
                    "player_name": {"type": "string"},
                    "POS": {"type": "string"},
                    "AGE": {"type": "string"},
                    "HT": {"type": "string"},
                    "WT": {"type": "string"},
                    "NAT": {"type": "string"},
                    "APP": {"type": "string"},
                    "SUB": {"type": "string"},
                    "SV": {"type": "string"},
                    "G": {"type": "string"},
                    "A": {"type": "string"},
                    "SH": {"type": "string"},
                    "ST": {"type": "string"},
                    "FC": {"type": "string"},
                    "FA": {"type": "string"},
                    "YC": {"type": "string"},
                    "RC": {"type": "string"},
                },
                "required": [
                    "player_name",
                    "POS",
                    "AGE",
                    "HT",
                    "WT",
                    "NAT",
                    "APP",
                    "SUB",
                    "G",
                    "A",
                    "SH",
                    "ST",
                    "FC",
                    "FA",
                    "YC",
                    "RC",
                ],
            },
        },
       
    },
    "required": ["squad_name", "goalkeepers"],
}

schema_test3 = {
    "properties": {
        "goalkeepers": {
            "type": "array",
            "items": {
                "type": "array",

                "items": {
                    "type": "object",
                    "properties": {
                        "NAME": {"type": "string"},
                        "NUMBER": {"type": "string"},
                        "POS": {"type": "string"},
                        "AGE": {"type": "string"},
                        "HT": {"type": "string"},
                        "WT": {"type": "string"},
                        "NAT": {"type": "string"},
                        "APP": {"type": "string"},
                        "SUB": {"type": "integer"},
                        "SV": {"type": "integer"},
                        "GA": {"type": "integer"},
                        "A": {"type": "integer"},
                        "FC": {"type": "integer"},
                        "FA": {"type": "integer"},
                        "YC": {"type": "integer"},
                        "RC": {"type": "integer"},
                    },
                    "required": ["NAME", "NUMBER","POS", "AGE", "HT", "WT", "NAT", "APP", "SUB", "SV", "GA", "A", "FC", "FA", "YC", "RC"],
                },
            },
        },
        "outfield_players": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                "type": "object",
                    "properties": {
                        "NAME": {"type": "string"},
                        "NUMBER": {"type": "string"},
                        "POS": {"type": "string"},
                        "AGE": {"type": "string"},
                        "HT": {"type": "string"},
                        "WT": {"type": "string"},
                        "NAT": {"type": "string"},
                        "APP": {"type": "string"},
                        "SUB": {"type": "integer"},
                        "G": {"type": "integer"},
                        "A": {"type": "integer"},
                        "SH": {"type": "integer"},
                        "ST": {"type": "integer"},
                        "FC": {"type": "integer"},
                        "FA": {"type": "integer"},
                        "YC": {"type": "integer"},
                        "RC": {"type": "integer"},
                    },
                    "required": ["NAME","NUMBER","POS", "AGE", "HT", "WT", "NAT", "APP", "SUB", "G", "A", "SH", "ST", "FC", "FA", "YC", "RC"],
                },
            },
        },
    },
    "required": ["goalkeepers", "outfield_players"],
}



schema_test = {
    "properties": {
        "goalkeepers": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
            },

          
        },
        "outfield_players": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "NAME": {"type": "string"},
                        "POS": {"type": "string"},
                        "AGE": {"type": "string"},
                        "HT": {"type": "string"},
                        "WT": {"type": "string"},
                        "NAT": {"type": "string"},
                        "APP": {"type": "string"},
                        "SUB": {"type": "string"},
                        "G": {"type": "string"},
                        "A": {"type": "string"},
                        "SH": {"type": "string"},
                        "ST": {"type": "string"},
                        "FC": {"type": "string"},
                        "FA": {"type": "string"},
                        "YC": {"type": "string"},
                        "RC": {"type": "string"},
                    },
                },
            },
        },
    },
    "required": ["goalkeepers", "outfield_players"]
}


schema_a = {
    "properties": {
        "regular_season": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "position": {"type": "integer"},
                    "team_name": {"type": "string"},
                    "MJ": {"type": "integer"},
                    "V": {"type": "integer"},
                    "N": {"type": "integer"},
                    "D": {"type": "integer"},
                    "BM": {"type": "integer"},
                    "BE": {"type": "integer"},
                    "DB": {"type": "integer"},
                    "Pts": {"type": "integer"},
                    "Pts/MJ": {"type": "number"},
                    "xG": {"type": "number"},
                    "xGA": {"type": "number"},
                    "xGD": {"type": "number"},
                    "xGD/90": {"type": "number"},
                    "5 derniers": {"type": "string"},
                    "Affluence": {"type": "string"},
                    "Meilleur buteur de l'équipe": {"type": "string"},
                    "Gardien de but": {"type": "string"},
                    "Notes": {"type": "string"}
                },
                "required": [
                    "position", "team_name", "MJ", "V", "N", "D", "BM", "BE", "DB",
                    "Pts", "Pts/MJ", "xG", "xGA", "xGD", "xGD/90", "5 derniers",
                    "Affluence", "Meilleur buteur de l'équipe", "Gardien de but", "Notes"
                ]
            }
        } 
    }
}






def extract(content: str, schema: dict):
    return create_extraction_chain(schema=schema, llm=llm, verbose=True).run(content)


def scrape_with_playwright(urls, schema):
    
    loader = AsyncChromiumLoader(urls)
    docs = loader.load()
    bs_transformer = BeautifulSoupTransformer()
    docs_transformed = bs_transformer.transform_documents(docs,tags_to_extract=["span"])
    print("Extracting content with LLM")

    # Grab the first 1000 tokens of the site
    splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(chunk_size=1000, 
                                                                    chunk_overlap=0, seperator="Regular season")
    splits = splitter.split_documents(docs_transformed)
    
    # Process the first split 
    extracted_content = extract(
        schema=schema, content=splits[0].page_content
    )
    pprint.pprint(extracted_content)
    return extracted_content


def main():

    # Recuperer le championnat
    #urls = ["https://www.espn.com/soccer/table/_/league/eng.1"]
    #extracted_content = scrape_with_playwright(urls, schema=schema)    
    #print(extracted_content)
    #df = pd.DataFrame(extracted_content[0]["table"])
    #print(df)
    #df.to_csv("table.csv")

    # Recuperer les resultats de l'equipe
    #urls = ["https://www.espn.com/soccer/team/results/_/id/382/eng.man_city"]
    #extracted_content = scrape_with_playwright(urls, schema=schema_equipe_result)    
    #print(extracted_content)
    #df = pd.DataFrame(extracted_content[0]["table"])
    #print(df)
    #df.to_csv("city_result.csv")

    urls = ["https://fbref.com/fr/comps/9/Statistiques-Premier-League"]
    extracted_content = scrape_with_playwright(urls, schema=schema_a)
    print(extracted_content)

if __name__ == "__main__": 
    main()