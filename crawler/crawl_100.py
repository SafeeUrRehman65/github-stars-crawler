import os
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

GITHUB_API_URL = "https://api.github.com/graphql"
TOKEN  = os.getenv("GITHUB_TOKEN")


QUERY = """
query($cursor: String) {
  search(query: "stars:>0", type: REPOSITORY, first: 100, after: $cursor) {
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        name
        owner {
          login
        }
        stargazerCount
      }
    }
  }
}
"""

def fetch_repos():
    headers = {"Authorization": f"Bearer {TOKEN}"}
    variables = {"cursor": None}

    response = requests.post(
        GITHUB_API_URL,
        json={"query": QUERY, "variables": variables},
        headers=headers
    )

    response.raise_for_status()
    data = response.json()
    return data["data"]["search"]["nodes"]

def save_to_db(repos):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        database=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        port=os.getenv("PGPORT")
    )
    cur = conn.cursor()

    rows = [(r["owner"]["login"], r["name"], r["stargazerCount"]) for r in repos]

    sql = """
    INSERT INTO repositories (owner, name, stars)
    VALUES %s
    ON CONFLICT(owner, name)
    DO UPDATE SET stars = EXCLUDED.stars, last_updated = now();
    """

    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    repos = fetch_repos()
    print(f"Fetched {len(repos)} repositories")
    save_to_db(repos)
    print("Saved to database!")