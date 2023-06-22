# Software engineering best practices for Databricks notebooks

This repository is a companion for the example article "Software engineering best practices for Databricks notebooks" ([AWS](https://docs.databricks.com/notebooks/best-practices.html) | [Azure](https://docs.microsoft.com/azure/databricks/notebooks/best-practices) | [GCP](https://docs.gcp.databricks.com/notebooks/best-practices.html)).

Going through the example, you will:

* Add notebooks to Databricks Repos for version control.
* Extracts portions of code from one of the notebooks into a shareable component.
* Test the shared code.
* Automatically run notebooks in git on a schedule using a Databricks job.
* Optionally, apply CI/CD to the notebooks and the shared code.


# Unravel Azure DevOps Integration

## APIs
Unravel API and Azure DevOps API using access tokens

## Additon to Azure DevOps Repository 

Need to add a pipeline YAML file and a CI/CD Python script.

azure-pipelines.yml

cicd-scripts/azdevopsclient.py 




## Github Action in Azure DevOps git

Need to create a repository branch policy to trigger a build on Pull Request.

https://learn.microsoft.com/en-us/azure/devops/repos/git/branch-policies?view=azure-devops&tabs=browser#build-validation -u "${{ vars.unravel_url' }}" -t "${{ screts.unravel_token }} -i 





# Comments
The example is hands-on. 


