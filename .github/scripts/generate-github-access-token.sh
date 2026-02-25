#!/usr/bin/env bash

# Inspired by implementation by Will Haley at:
#   http://willhaley.com/blog/generate-jwt-with-bash/

# Stolen from
#   https://stackoverflow.com/questions/46657001/how-do-you-create-an-rs256-jwt-assertion-with-bash-shell-scripting
# and simplified to suit our needs

set -euo pipefail

app_id=${APP_ID?"Need to set APP_ID"}                                        # The app id of the app
app_installation_id=${APP_INSTALLATION_ID?"Need to set APP_INSTALLATION_ID"} # The installation id of the app
siging_key_path=${SIGNING_KEY_PATH?"Need to set SIGNING_KEY_PATH"}           # Path to the private key used to sign the JWT as a pem file

header_template='{
    "typ": "JWT",
    "kid": "0001",
    "iss": "https://gist.github.com/Nastaliss/7f8466f59072d744540190721a63672d"
}'

build_header() {
    jq -c \
        --arg iat_str "$(date +%s)" \
        --arg alg RS256 \
        '
        ($iat_str | tonumber) as $iat
        | .alg = $alg
        | .iat = $iat
        | .exp = ($iat + 1)
        ' <<<"$header_template" | tr -d '\n'
}

b64enc() { openssl enc -base64 -A | tr '+/' '-_' | tr -d '='; }
json() { jq -c . | LC_CTYPE=C tr -d '\n'; }

sign() {
    local payload header sig secret=$2
    header=$(build_header) || return
    payload=${1}
    signed_content="$(json <<<"$header" | b64enc).$(json <<<"$payload" | b64enc)"
    sig=$(printf %s "$signed_content" | openssl dgst -binary -sha256 -sign <(printf '%s\n' "$secret") | b64enc)
    printf '%s.%s\n' "${signed_content}" "${sig}"
}

# Construct the payload
# Max duration is one hour, account for clock skew by subtracting 10 seconds
payload="{\"iat\": $(($(date +%s) - 10)),\"exp\": $(($(date +%s) - 10 + 10 * 60)),\"iss\": \"${app_id}\"}"

# Get the secret from a file
# The secret needs to be a pem file with line breaks, with or without leading/trailing line break
secret=$(cat "$siging_key_path")

# Generate a jwt, according to https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app
# and https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-json-web-token-jwt-for-a-github-app#generating-a-json-web-token-jwt
jwt=$(sign "$payload" "$secret")

# Actually get an access token from the github api
curl -s --location --request POST "https://api.github.com/app/installations/$app_installation_id/access_tokens" \
    --header "Authorization: Bearer $jwt" \
    --header 'Accept: application/vnd.github+json' \
    --header 'X-GitHub-Api-Version: 2022-11-28' | jq ".token" | tr -d '"'
