from kanoniv import Spec

# From a file
spec = Spec.from_file("identity-spec.yaml")

# From a YAML string
spec = Spec.from_string("""
api_version: kanoniv/v1
identity_version: "1.0"
entity:
  name: customer
sources:
  crm:
    adapter: csv
    location: contacts.csv
    primary_key: crm_id
    attributes:
      email: email_address
      phone: phone_number
rules:
  - name: email_exact
    type: exact
    field: email
    weight: 1.0
decision:
  thresholds:
    match: 0.9
    review: 0.7
""")