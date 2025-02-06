# We use the following secrets:

Don't forget to check thtat everthing is working and configured on the
https://github.com/EPFL-ENAC/AddLidar-API/settings/secrets/actions repository_secrets tab

## For the Continuous Deployment pipeline (on our kube cluster):

Btw the path of the config is located here
cd $repo_org/$repo_name/overlays/$branch

(cf https://github.com/EPFL-ENAC/enack8s-app-config?tab=readme-ov-file#setting-secrets for how to setup)
CD_ORG = "epfl-resslab" # The organisation folder for the repository manifests in enack8s-app-config ie. epfl-resslab-astra-82001
CD_REPO = "AddLidar-API_frontend" # The repository name for the manifests in enack8s-app-config ie.
CD_TOKEN = "your-cd-token-secret"
CD_URI = "[your-uri](https://api.github.com/repos/EPFL-ENAC/enack8s-app-config/dispatches)" # The URI of the webhook that establishes the manifest code updating

## For the automatic release using google release-please github action

MY_RELEASE_PLEASE_TOKEN = "your-my-release-please-token-secret"
