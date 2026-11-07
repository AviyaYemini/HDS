#!/usr/bin/env bash
# Alertmanager's configuration and e-mails (specs/006-deploy-operations T056, FR-010a; reviews R159
# to R165), without a cluster. Run by the CI chart job: infra/tests/check_alertmanager.sh
#   1. The Vault Secrets Operator's template (hds-secrets, files/alertmanager.yaml.tmpl, as the
#      chart renders it) inserts values only as `{{ .Secrets.X | toJson }}` or the smarthost's
#      printf form, so no value can break the YAML or add to it.
#   2. Filled as the operator would fill it, with hostile values (a password holding a quote, a
#      colon, a hash and a newline; two addresses), it passes `amtool check-config` with the
#      message templates, and the parsed values are the input, byte for byte.
#   3. One receiver, e-mail, and no sub-routes: every alert is sent, Flux's too.
#   4. The e-mail names the alert, the object, the time and the summary, and nothing else.
# Helm, yq and Alertmanager run from images pinned in infra/VERSIONS.md.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
HELM_IMAGE="alpine/helm:3.22.1@sha256:8191701ad1f82ee3695c87211fcbfe6d7b1bb7df92ac1c8de9ccdb0995a6a2c1"
YQ_IMAGE="mikefarah/yq:4.54.1@sha256:4b3d9475d65571d28cbb19544d3820ec2945e4c8b2f18279394282b8dc3a592e"
ALERTMANAGER_IMAGE="quay.io/prometheus/alertmanager:v0.34.1@sha256:e9733bafb1bdef9b00e25a21f8f99dc26a22224bf16641ad754d1649f4c3357a"
work=$(mktemp -d)
trap 'rm -rf -- "${work:?}"' EXIT
chmod 755 "$work"
mkdir -p "$work/cfg" "$work/templates"

failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; [ -z "${2:-}" ] || printf '      %s\n' "$2"; failures=$((failures + 1)); }
same() { if [ "$2" = "$3" ]; then pass "$1"; else flunk "$1" "wanted: $2; got: $3"; fi; }
yq() { docker run --rm -i "$YQ_IMAGE" "$@"; }
amtool() {
    docker run --rm --entrypoint amtool -v "$work/cfg:/cfg:ro" \
        -v "$work/templates:/etc/alertmanager/configmaps/alertmanager-hds-templates:ro" \
        "$ALERTMANAGER_IMAGE" "$@"
}

docker run --rm -e HELM_CACHE_HOME=/tmp -e HELM_CONFIG_HOME=/tmp -e HELM_DATA_HOME=/tmp \
    -v "$ROOT/infra/helm:/charts:ro" -w /charts "$HELM_IMAGE" \
    template hds-secrets hds-secrets --namespace hds --show-only templates/monitoring.yaml \
    > "$work/rendered.yaml"
yq e 'select(.kind == "VaultStaticSecret") | .spec.destination.transformation.templates["alertmanager.yaml"].text' \
    < "$work/rendered.yaml" > "$work/template"
yq e 'select(.kind == "ConfigMap") | .data["hds.tmpl"]' < "$work/rendered.yaml" > "$work/templates/hds.tmpl"

echo "== the operator's template"
insertions=$(grep -o '{{[^}]*}}' "$work/template" | sort -u)
allowed='^\{\{ \.Secrets\.(OPERATOR_EMAILS|MAIL_FROM|SMTP_USERNAME|SMTP_PASSWORD) \| toJson \}\}$|^\{\{ printf "%s:%s" \.Secrets\.SMTP_HOST \.Secrets\.SMTP_PORT \| toJson \}\}$'
bad=$(grep -Ev "$allowed" <<< "$insertions" || true)
same "every value is inserted as a JSON string, nothing else" "" "$bad"
same "it inserts the six settings" 6 "$(grep -o '\.Secrets\.[A-Z_]*' "$work/template" | sort -u | wc -l | tr -d ' ')"

# The operator's rendering, as text/template would do it for these forms: each value as JSON.
password=$'pa"ss: y # z\n*then: more'
emails="ops-one@example.org, ops-two@example.org"
jq -n -r --rawfile t "$work/template" --arg password "$password" --arg emails "$emails" '
    {OPERATOR_EMAILS: $emails, MAIL_FROM: "hds@example.org", SMTP_USERNAME: "hds@example.org",
     SMTP_PASSWORD: $password, SMTP_HOST: "smtp.example.org", SMTP_PORT: "587"} as $s
    | $t
    | gsub("\\{\\{ printf \"%s:%s\" \\.Secrets\\.SMTP_HOST \\.Secrets\\.SMTP_PORT \\| toJson \\}\\}";
        ($s.SMTP_HOST + ":" + $s.SMTP_PORT) | tojson)
    | reduce ($s | keys[]) as $k (.; gsub("\\{\\{ \\.Secrets\\." + $k + " \\| toJson \\}\\}"; $s[$k] | tojson))
' > "$work/cfg/alertmanager.yaml"

echo "== the configuration, filled with hostile values"
if out=$(amtool check-config /cfg/alertmanager.yaml 2>&1); then
    pass "amtool check-config passes, with the message templates"
else
    flunk "amtool check-config" "$out"
fi
email() { yq -o=json e ".receivers[0].email_configs[0].$1" < "$work/cfg/alertmanager.yaml" | tr -d '\n'; }
same "the password is the input, byte for byte" "$(jq -n --arg v "$password" '$v' | tr -d '\n')" "$(email auth_password)"
same "the two addresses are one 'to'" "$(jq -n --arg v "$emails" '$v' | tr -d '\n')" "$(email to)"
same "the smarthost is host:port" '"smtp.example.org:587"' "$(email smarthost)"
same "TLS is required" "true true" \
    "$(yq e '.global.smtp_require_tls' < "$work/cfg/alertmanager.yaml") $(yq e '.receivers[0].email_configs[0].require_tls' < "$work/cfg/alertmanager.yaml")"
same "nothing else got into the file" "global receivers route templates" \
    "$(yq e 'keys | sort | join(" ")' < "$work/cfg/alertmanager.yaml")"

echo "== the route"
same "one receiver, by e-mail, and no sub-route, so Flux's alerts are sent too" "operators operators 1 0 0" \
    "$(yq e '.route.receiver + " " + .receivers[0].name + " " + (.receivers[0].email_configs | length | tostring) + " " + ((.route.routes // []) | length | tostring) + " " + ((.receivers[0].webhook_configs // []) | length | tostring)' < "$work/cfg/alertmanager.yaml")"
flux_route=$(amtool config routes test --config.file=/cfg/alertmanager.yaml alertname=FluxHelmReleaseUpgradefailed kind=HelmRelease name=hds namespace=flux-system 2>&1 || true)
same "a Flux alert goes to the operators" operators "$(tr -d '\r' <<< "$flux_route")"

echo "== the e-mail"
cat > "$work/cfg/data.json" <<'EOF'
{"receiver": "operators", "status": "firing", "externalURL": "http://alertmanager.example",
 "groupLabels": {"alertname": "HdsBackupFailed"},
 "commonLabels": {"alertname": "HdsBackupFailed", "namespace": "hds", "severity": "page"},
 "commonAnnotations": {},
 "alerts": [
  {"status": "firing", "startsAt": "2026-11-07T03:30:00Z", "endsAt": "0001-01-01T00:00:00Z",
   "generatorURL": "http://prometheus.example/graph?g0.expr=secret-looking",
   "fingerprint": "0123456789abcdef",
   "labels": {"alertname": "HdsBackupFailed", "namespace": "hds", "job_name": "hds-backup-29001",
     "owner_name": "hds-backup", "severity": "page", "instance": "10.42.0.7:8080"},
   "annotations": {"summary": "The newest backup (hds-backup-29001) failed: on the machine, off it, or both."}},
  {"status": "firing", "startsAt": "2026-11-07T04:00:00Z", "endsAt": "0001-01-01T00:00:00Z",
   "generatorURL": "", "fingerprint": "fedcba9876543210",
   "labels": {"alertname": "FluxHelmReleaseUpgradefailed", "kind": "HelmRelease", "name": "hds",
     "namespace": "flux-system", "severity": "error"},
   "annotations": {"message": "Helm upgrade failed: timed out waiting for the condition"}}
 ]}
EOF
render() {
    amtool template render --template.glob='/etc/alertmanager/configmaps/alertmanager-hds-templates/*.tmpl' \
        --template.data=/cfg/data.json --template.text="$1" 2>&1
}
subject=$(render '{{ template "email.default.subject" . }}')
same "the subject" "[HDS] 2 firing: HdsBackupFailed" "$subject"
body=$(render '{{ template "email.default.html" . }}')
for want in "<b>HdsBackupFailed</b>" "Job hds-backup-29001, since 2026-11-07 03:30 UTC" \
    "The newest backup (hds-backup-29001) failed" "<b>FluxHelmReleaseUpgradefailed</b>" \
    "HelmRelease flux-system/hds, since 2026-11-07 04:00 UTC" "Helm upgrade failed: timed out"; do
    if grep -qF -- "$want" <<< "$body"; then pass "the body has: $want"; else flunk "the body has: $want" "$body"; fi
done
for unwanted in "secret-looking" "10.42.0.7" "owner_name" "severity" "http://"; do
    if grep -qF -- "$unwanted" <<< "$body"; then flunk "the body leaves out: $unwanted" "$body"; else pass "the body leaves out: $unwanted"; fi
done

echo "== resolved e-mails (review R186)"
# render_with DATA_FILE TEXT: a template against another set of alerts.
render_with() {
    amtool template render --template.glob='/etc/alertmanager/configmaps/alertmanager-hds-templates/*.tmpl' \
        --template.data="/cfg/$1" --template.text="$2" 2>&1
}
cat > "$work/cfg/flux-resolved.json" <<'EOF_DATA'
{"receiver": "operators", "status": "resolved", "externalURL": "http://alertmanager.example",
 "groupLabels": {"alertname": "FluxHelmReleaseUpgradefailed"},
 "commonLabels": {"alertname": "FluxHelmReleaseUpgradefailed", "kind": "HelmRelease", "name": "hds", "namespace": "flux-system"},
 "commonAnnotations": {},
 "alerts": [{"status": "resolved", "startsAt": "2026-11-07T04:00:00Z", "endsAt": "2026-11-07T04:05:00Z",
   "generatorURL": "", "fingerprint": "fedcba9876543210",
   "labels": {"alertname": "FluxHelmReleaseUpgradefailed", "kind": "HelmRelease", "name": "hds", "namespace": "flux-system"},
   "annotations": {"message": "Helm upgrade failed: timed out waiting for the condition"}}]}
EOF_DATA
sed 's/"status": "firing"/"status": "resolved"/g' "$work/cfg/data.json" \
    | jq '.alerts |= map(select(.labels.alertname | startswith("Hds")))' > "$work/cfg/hds-resolved.json"
same "a resolved Flux alert's subject does not say fixed" \
    "[HDS] no longer recent, not fixed: FluxHelmReleaseUpgradefailed" \
    "$(render_with flux-resolved.json '{{ template "email.default.subject" . }}')"
flux_body=$(render_with flux-resolved.json '{{ template "email.default.html" . }}')
for want in "(no longer recent, not fixed)" "it may still be failing. Run make ops-status"; do
    if grep -qF -- "$want" <<< "$flux_body"; then pass "a resolved Flux alert's body: $want"; else flunk "a resolved Flux alert's body: $want" "$flux_body"; fi
done
same "a resolved HDS alert still says resolved" "[HDS] resolved: HdsBackupFailed" \
    "$(render_with hds-resolved.json '{{ template "email.default.subject" . }}')"
hds_body=$(render_with hds-resolved.json '{{ template "email.default.html" . }}')
if grep -qF "(resolved)" <<< "$hds_body" && ! grep -qF "not fixed" <<< "$hds_body"; then
    pass "and its body says resolved, without the Flux note"
else
    flunk "and its body says resolved, without the Flux note" "$hds_body"
fi

[ "$failures" -eq 0 ] || { echo "alertmanager checks: $failures failed" >&2; exit 1; }
echo "alertmanager checks: all passed"
