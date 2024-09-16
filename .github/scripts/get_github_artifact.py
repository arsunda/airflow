import requests
import os

def list_artifacts(repo_owner, repo_name, github_token):
    """
    List all artifacts in a GitHub repository.

    :param repo_owner: Owner of the repository.
    :param repo_name: Name of the repository.
    :param github_token: GitHub Personal Access Token.
    :return: List of artifact names and IDs.
    """
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/artifacts"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        artifacts = response.json().get('artifacts', [])
        return [(artifact['id'], artifact['name']) for artifact in artifacts]
    else:
        raise Exception(f"Failed to list artifacts: {response.status_code} {response.text}")

# Example usage
if __name__ == "__main__":
    repo_owner = os.getenv("REPO_OWNER")
    repo_name = os.getenv("REPO_NAME")
    github_token = os.getenv("GITHUB_TOKEN")

    artifacts = list_artifacts(repo_owner, repo_name, github_token)
    for artifact_id, artifact_name in artifacts:
        print(f"Artifact ID: {artifact_id}, Name: {artifact_name}")
