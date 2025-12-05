#!/usr/bin/env python3
import os

def main():
    # ensure all variables are set, error message should include unset variables
    unset_vars = [
        var
        for var in [
            "GITHUB_RUNNER_TOKEN",
            "RUNNER_NAME",
            "ORGANIZATION_NAME",
            "REPOSITORY_NAME",
            "RUNNER_LABELS",
        ]
        if not os.environ.get(var)
    ]
    if unset_vars:
        raise ValueError(f"Variables {', '.join(unset_vars)} must be set")

    registration_token = os.environ["GITHUB_RUNNER_TOKEN"]
    runner_name = f"{os.environ['RUNNER_HOSTNAME']}-{os.environ['RUNNER_NAME']}"
    org_name = os.environ["ORGANIZATION_NAME"]
    repo_name = os.environ["REPOSITORY_NAME"]
    runner_labels = os.environ["RUNNER_LABELS"]
    
    # allow running as root
    os.environ["RUNNER_ALLOW_RUNASROOT"] = "1"

    print("Configuring the runner...")
    # Execute /actions-runner/config.sh with the token and labels
    config_command = f"/actions-runner/config.sh --url https://github.com/{org_name}/{repo_name} --token {registration_token} --name {runner_name} --replace --labels {runner_labels} --unattended"
    os.system(config_command)
    
     # Start the runner
    print("Starting the runner...")
    os.execv("/bin/bash", ["/bin/bash", "/actions-runner/run.sh"])

if __name__ == "__main__":
    main()
