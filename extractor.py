import json
import logging
import numpy as np

from pathlib import Path
from typing import List, Dict
from os import environ, mkdir, path
from python_graphql_client import GraphqlClient


token = "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"
BASE_DIR = Path(__file__).parents[1]
TOKEN = "GITHUB_API_TOKEN"

def _set_repositories(repositories_filename: str) -> list:
    """Formats the list of repositories to be extracted from GitHub. 
    The elegible repositories come from a filename in resources/.
    """
    repositories = []
    try:
        with open(repositories_filename, "r") as file:
            repos = file.readlines()
    except FileNotFoundError:
        raise FileNotFoundError(f"Could not open/read {repositories_filename}")

    for i, item in enumerate(repos):
        if i > 0:
            repo = item.rstrip().split()
            repositories.append(dict(owner=repo[0], name=repo[1]))

    return repositories


def setup() -> dict:
    """Sets up the necessary information for GitHub API discussions extraction.

    Returns
    -------
    dict
        Dictionary structure including GitHub API URL, its headers, and 
        repositories to be extracted.

    Raises
    ------
    KeyError
        Occurs when the GitHub API token is not set as an environment variable. 
    """
    resources_path = path.join(BASE_DIR, "resources")

    try:
        api_token = environ[TOKEN]
    except KeyError:
        raise KeyError(f"Could not find env variable {TOKEN}. Set it first.")

    repositories_filename = path.join(resources_path, "repositories.txt")
    repositories = _set_repositories(repositories_filename)

    url = "https://api.github.com/graphql"
    headers = {"Authorization": "bearer %s " % api_token}

    return dict(
        url=url,
        headers=headers,
        repositories=repositories,
    )


def discussions_query() -> str:
    """Structures query to extract discussions.
    """
    return """
        query($repo_owner: String!, $repo_name: String!, $discussions_first: Int!, $discussions_after: String) {
            repository(owner: $repo_owner, name: $repo_name) {
                discussions(first: $discussions_first, after: $discussions_after, orderBy: {field: CREATED_AT, direction: DESC}) {
                    # type: DiscussionConnection
                    totalCount # Int!

                    pageInfo {
                        # type: PageInfo (from the public schema)
                        startCursor
                        endCursor
                        hasNextPage
                        hasPreviousPage
                    }

                    edges {
                        # type: DiscussionEdge
                        cursor
                        node {
                            # type: Discussion
                            id
                            number
                            url
                            title
                            bodyHTML
                            bodyText
                            publishedAt
                            upvoteCount
                            category {
                                name
                            }
                            reactionGroups {
                                content
                                reactors(first: 10) {
                                    totalCount
                                }
                            }
                            answer {
                                # type: DiscussionComment
                                id
                                url
                                bodyHTML
                                publishedAt
                                upvoteCount
                            }
                            comments(first: 10) {
                                # type: DiscussionCommentConnection
                                totalCount
                            }
                        }
                    }
                    nodes {
                        # type: Discussion
                        id
                    }
                }
            }
        }"""


def discussion_comments_query() -> str:
    """Structures query to extract comments for a given discussion_id.
    """
    return """
        query($discussion_id: ID!, $comments_first: Int!, $comments_after: String) {
            node(id: $discussion_id) {
                ... on Discussion {
                    comments(first: $comments_first, after: $comments_after) {
                        # type: DiscussionCommentConnection
                        totalCount
                
                        pageInfo {
                            # type: PageInfo (from the public schema)
                            startCursor
                            endCursor
                            hasNextPage
                            hasPreviousPage
                        }
                        edges {
                            # type: DiscussionCommentEdge
                            cursor
                            node {
                                # type: DiscussionComment
                                id
                                databaseId
                                url
                                bodyHTML
                                bodyText
                                publishedAt
                                isAnswer
                                upvoteCount
                                reactionGroups {
                                    content
                                    reactors(first: 10) {
                                        totalCount
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }"""


def fetch_discussion_comments(client: GraphqlClient,
                              discussion_id: str) -> List[Dict[str, any]]:
    """Fetches comments given a discussion node id. It also paginates on 
    comments and each page has a maximum of 100 comments, by GitHub API 
    conventions.

    Parameters
    ----------
    client : GraphqlClient
        GitHub API active client.
    discussion_id : str
        Node id of a discussion.

    Returns
    -------
    List[Dict[str, any]]
        List of comments extracted given a discussion_id, in JSON format.
    """
    comments = []
    has_next_page = True
    variables = {
        "discussion_id": discussion_id,
        "comments_first": 100,
        "comments_after": None,
    }

    while has_next_page:
        data = client.execute(query=discussion_comments_query(),
                              variables=variables,
                              headers=headers)

        comments += data["data"]["node"]["comments"]["edges"]

        # Handle comments pagination
        has_next_page = data["data"]["node"]["comments"]["pageInfo"][
            "hasNextPage"]
        variables["comments_after"] = data["data"]["node"]["comments"][
            "pageInfo"]["endCursor"]

    return comments


def fetch_discussions(client: GraphqlClient, headers: Dict[str, any],
                      repository: Dict[str, any]) -> Dict[str, any]:
    """Fetches all the discussions from a repository and save them in data/raw/
    repository_name. It also paginates on discussions and each page has a 
    maximum of 100 discussions, by GitHub API conventions.

    Parameters
    ----------
    client : GraphqlClient
        Client to connect with GitHub GraphQL API.
    headers : Dict[str, any]
        Authorization API token necessary to access GitHub API.
    repository : Dict[str, any]
        Owner and name of the repository to be extracted.

    Returns
    -------
    Dict[str, any]
        Discussions, its comments and other important features in a JSON 
        structure.

    Raises
    ------
    Exception
        Occurs when some error happens during client requests.
    """
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    checkpoints = np.around(np.linspace(0, 1, 11),
                            decimals=1).tolist()    # For logging progress

    discussions = []
    has_next_page = True
    count = 0
    variables = {
        "repo_name": repository["name"],
        "repo_owner": repository["owner"],
        "discussions_first": 100,
        "discussions_after": None,
    }

    while has_next_page:
        data = client.execute(query=discussions_query(),
                              variables=variables,
                              headers=headers)

        if "errors" in data:
            raise Exception(
                f"Error when fetching discussions: {data['errors']}")

        # Fetch comments for each discussion
        for discussion in data["data"]["repository"]["discussions"]["edges"]:
            if discussion["node"]["comments"]["totalCount"] > 0:
                discussion["node"]["comments"][
                    "edges"] = fetch_discussion_comments(
                        client=client, discussion_id=discussion["node"]["id"])

        # Append current discussions to the resulting list of discussions
        discussions += data["data"]["repository"]["discussions"]["edges"]
        has_next_page = data["data"]["repository"]["discussions"]["pageInfo"][
            "hasNextPage"]
        variables["discussions_after"] = data["data"]["repository"][
            "discussions"]["pageInfo"]["endCursor"]
        total_count = data["data"]["repository"]["discussions"]["totalCount"]

        # Progress bar and logging
        progress = np.around(count / float(round(total_count / 100)),
                             decimals=1)
        if progress in checkpoints:
            checkpoints.pop(0)

            bar_length = 100
            filled_up_length = int(round(bar_length * progress))

            bar = '=' * filled_up_length + '-' * (bar_length - filled_up_length)
            logging.info(
                f'Processing {repository["name"]} [{bar}] page {count}/{round(total_count/100)}'
            )

        if count % 10 == 0:
            print(len(discussions))
            _save_repository_extraction(repository=repository, data=discussions)
            discussions = []

        count += 1

    if discussions:
        print(len(discussions))
        _save_repository_extraction(repository=repository, data=discussions)


def _save_repository_extraction(repository, data):
    """Saves a batch of discussions in data/raw/repository_name folder. It 
    saves each discussion in json format as a file named by its discussion 
    number.
    """
    repository_path = path.join(BASE_DIR, "data", "raw", repository["name"])
    if not path.exists(repository_path):
        mkdir(repository_path)

    for discussion in data:
        discussion_number = discussion["node"]["number"]
      
        # Format reactions data
        discussion_reactions = []
        for reaction in discussion["node"]["reactionGroups"]:
            discussion_reactions.append({
                "reaction":
                reaction["content"],
                "totalCount":
                reaction["reactors"]["totalCount"],
            })
        # Format comments data
        comments = []
        if discussion["node"]["comments"]["totalCount"] > 0:
            for comment in discussion["node"]["comments"]["edges"]:
                comment_reactions = []
                for reaction in comment["node"]["reactionGroups"]:
                    comment_reactions.append({
                        "reaction":
                        reaction["content"],
                        "totalCount":
                        reaction["reactors"]["totalCount"],
                    })
                comments.append({
                    "id": comment["node"]["id"],
                    "databaseId": comment["node"]["databaseId"],
                    "url": comment["node"]["url"],
                    "bodyHTML": comment["node"]["bodyHTML"],
                    "date": comment["node"]["publishedAt"],
                    "isAnswer": comment["node"]["isAnswer"],
                    "upvoteCount": comment["node"]["upvoteCount"],
                    "reactions": comment_reactions,
                })

        # Format final discussion data to be saved
        discussion_data = {
            "id": discussion["node"]["id"],
            "url": discussion["node"]["url"],
            "title": discussion["node"]["title"],
            "bodyHTML": discussion["node"]["bodyHTML"],
            "date": discussion["node"]["publishedAt"],
            "upvoteCount": discussion["node"]["upvoteCount"],
            "category": discussion["node"]["category"],
            "reactions": discussion_reactions,
            "answer": discussion["node"]["answer"],
            "comments": comments,
        }

        data_file = path.join(repository_path, str(discussion_number))
        with open(data_file, "w") as file:
            file.write(json.dumps(discussion_data, indent=2))


if __name__ == "__main__":
    configurations = setup()

    url = configurations.get("url")
    headers = configurations.get("headers")
    repositories = configurations.get("repositories")

    client = GraphqlClient(endpoint=url)

    for repo in repositories:
        fetch_discussions(client=client, headers=headers, repository=repo)
        #print(json.dumps(discussions, indent=2))
