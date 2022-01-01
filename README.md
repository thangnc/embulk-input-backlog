# embulk-input-backlog

The Embulk Input Plugin for [Backlog](https://backlog.com/)

[![CircleCI](https://circleci.com/gh/thangnc/embulk-input-backlog/tree/main.svg?style=shield)](https://circleci.com/gh/thangnc/embulk-input-backlog/?branch=main)
[![Coverage Status](https://coveralls.io/repos/github/thangnc/embulk-input-backlog/badge.svg?branch=main)](https://coveralls.io/github/thangnc/embulk-input-backlog?branch=main)

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: yes

## Configuration

- **auth_method**: Backlog auth method (string, `api_key` or `oauth2`, default: `api_key`)
- **api_key**: Backlog API key (string, required if `auth_method` is `api_key`)
- **access_token**: Backlog access token (string, required if `auth_method` is `oauth2`)
- **uri**: Backlog API endpoint. Your `https://spaceKey.backlog.com` or `https://spaceKey.backlogtool.com` or `https://spaceKey.backlog.jp` (string, required)
- **initial_retry_interval_millis**: Wait seconds for exponential backoff initial value (integer, default: 1)
- **retry_limit**: Try to retry this times (integer, default: 5)

## Example

```yaml
in:
  type: backlog
  auth_method: api_key
  api_key: my-test-api-key
  uri: https://test-plugin.backlog.com
  columns:
    - { name: id, type: long }
    - { name: issueKey, type: string }
    - { name: projectId, type: long }
    - { name: summary, type: string }
    - { name: assignee.name, type: string }
    - { name: status.name, type: string }
out:
  type: stdout
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
