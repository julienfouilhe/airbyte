# HubSpot Source

This is the repository for the HubSpot source connector, written in Python. For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.io/integrations/sources/hubspot).

## Primary keys

The primary key for the following streams is `id`:

- campaigns
- companies
- contacts
- deals
- email_events
- engaments
- engagements_calls
- engagements_emails
- engagements_meetings
- engagements_notes
- engagements_tasks
- forms
- goals
- line_items
- marketing_emails
- owners
- products
- tickets
- ticket_pipelines
- workflows
- quotes

The primary key for the following streams is `canonical-vid`:

- contacts_list_memberships

The primary key for the following streams is `pipelineId`:

- deal_pipelines

The primary key for the following streams is `vid-to-merge`:

- contacts_merged_audit

The following streams do not have a primary key:

- contact_lists (The primary key could potentially be a composite key (portalId, listId) - https://legacydocs.hubspot.com/docs/methods/lists/get_lists)
- form_submissions (The entities returned by this endpoint do not have an identifier field - https://legacydocs.hubspot.com/docs/methods/forms/get-submissions-for-a-form)
- subscription_changes (The entities returned by this endpoint do not have an identified field - https://legacydocs.hubspot.com/docs/methods/email/get_subscriptions_timeline)
- property_history (The entities returned by this endpoint do not have an identifier field - https://legacydocs.hubspot.com/docs/methods/contacts/get_contacts)

## Local development

### Prerequisites

**To iterate on this connector, make sure to complete this prerequisites section.**

#### Minimum Python version required `= 3.7.0`

#### Build & Activate Virtual Environment and install dependencies

From this connector directory, create a virtual environment:

```
python -m venv .venv
```

This will generate a virtualenv for this module in `.venv/`. Make sure this venv is active in your development environment of choice. To activate it from the terminal, run:

```
source .venv/bin/activate
pip install -r requirements.txt
```

If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

Note that while we are installing dependencies from `requirements.txt`, you should only edit `setup.py` for your dependencies. `requirements.txt` is used for editable installs (`pip install -e`) to pull in Python dependencies from the monorepo and will call `setup.py`. If this is mumbo jumbo to you, don't worry about it, just put your deps in `setup.py` but install using `pip install -r requirements.txt` and everything should work as you expect.

#### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.io/integrations/sources/hubspot)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_hubspot/spec.yaml` file. Note that the `secrets` directory is gitignored by default, so there is no danger of accidentally checking in sensitive information. See `sample_files/sample_config.json` for a sample config file.

**If you are an Airbyte core member**, copy the credentials in Lastpass under the secret name `source hubspot test creds`
and place them into `secrets/config.json`.

### Locally running the connector

```
python main.py spec
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog sample_files/basic_read_catalog.json
```


## Testing
You can run our full test suite locally using [`airbyte-ci`](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md):
```bash
airbyte-ci connectors --name=source-hubspot test
```

### Customizing acceptance Tests
Customize `acceptance-test-config.yml` file to configure tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.

## Dependency Management

All of your dependencies should go in `setup.py`, NOT `requirements.txt`. The requirements file is only used to connect internal Airbyte dependencies in the monorepo for local development.

### Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing our test suite: `airbyte-ci connectors --name=source-hubspot test`
2. Bump the connector version in `metadata.yaml`: increment the `dockerImageTag` value. Please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors).
3. Make sure the `metadata.yaml` content is up to date.
4. Make the connector documentation and its changelog is up to date (`docs/integrations/sources/hubspot.md`).
5. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
6. Pat yourself on the back for being an awesome contributor.
7. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.

