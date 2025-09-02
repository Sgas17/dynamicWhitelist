# dynamicWhitelist 


This project with create a dynamic whitelist of erc20 tokens to trade / monitor based on some pre-defined criteria.

This whitelist will be published to nats/redis for use by other services 

It will run scripts periodically to 
1 - update a database with any new tokens
2 - update the database with any new pools 

for both Base and Ethereum chains but ultimately agnostically 







##structure 

- src/scripts #scripts to scrape blockchain data

- src/processors #process scraped data

- src/utils #helper functions for project

- tests #project test suite

## GitHub Integration

The repository is configured with Claude Code GitHub Actions integration via `.github/workflows/claude.yml`. Claude can be triggered by:
- Comments containing `@claude` on issues or pull requests
- Issue creation with `@claude` in title or body
- Pull request review comments containing `@claude`

The workflow has permissions for:
- Repository content modification
- Pull request and issue management
- Reading CI results

## Development Commands

Since this is an early-stage project with no package.json, Dockerfile, or other configuration files, standard build/test/lint commands are not yet established. These will need to be defined as the project develops.