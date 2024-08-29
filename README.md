# mp-covenants-split-pages

This AWS Lambda function is part of [Mapping Prejudice's](https://mappingprejudice.umn.edu/) Deed Machine application. This component receives information about a newly uploaded or updated image, generally via an event triggered by a matching S3 upload event. The function examines the image to determine what, if any, reprocessing operations are necessary before the image can be fully processed by the Deed Machine.

The [Deed Machine](https://github.com/UMNLibraries/racial_covenants_processor/) is a multi-language set of tools that use OCR and crowdsourced transcription to identify racially restrictive covenant language, then map the results.

The Lambda components of the Deed Machine are built using Amazon's Serverless Application Model (SAM) and the AWS SAM CLI tool.

## Key links
- [License](https://github.com/UMNLibraries/racial_covenants_processor/blob/main/LICENSE)
- [Component documentation](https://the-deed-machine.readthedocs.io/en/latest/modules/lambdas/mp-covenants-split-pages.html)
- [Documentation home](https://the-deed-machine.readthedocs.io/en/latest/)
- [Downloadable Racial covenants data](https://github.com/umnlibraries/mp-us-racial-covenants)
- [Mapping Prejudice main site](https://mappingprejudice.umn.edu/)

## Software development requirements
- Pipenv (Can use other virtual environments, but will require fiddling on your part)
- AWS SAM CLI
- Docker
- Python 3

## Quickstart commands

To build the application:

You will need to make a copy of `samconfig.toml.sample` to set your actual config settings. Save this file as `samconfig.toml`. This file will be ignored by version control.

Then:

```bash
pipenv install
pipenv shell
sam build
```

To rebuild and deploy the application:

```bash
sam build && sam deploy
```

To run tests:

```bash
pytest
```