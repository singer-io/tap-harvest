# Changelog

## 2.1.3
  * Dependabot update [#58](https://github.com/singer-io/tap-harvest/pull/58)

## 2.1.2
  * Request Timeout Implemented [#51](https://github.com/singer-io/tap-harvest/pull/51)
## 2.1.1
  * Fixed list index out of range problem when no account [#44](https://github.com/singer-io/tap-harvest/pull/44)
## 2.1.0
  * Add support for python 3.9.6 [#48](https://github.com/singer-io/tap-harvest/pull/48)

## 2.0.8
  * Update request headers when querying for account IDs [#41](https://github.com/singer-io/tap-harvest/pull/41)

## 2.0.7
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.0.6
  * Uses date-time parsing/formatting to add times to dates, instead of naive append [#27](https://github.com/singer-io/tap-harvest/pull/27)

## 2.0.5
  * Fixes `estimates` stream to use `estimate.creator` instead of `estimate.user` [#26](https://github.com/singer-io/tap-harvest/pull/26)

## 2.0.4
  * Fixes schema of `external_reference.group_id` to be a string [#24](https://github.com/singer-io/tap-harvest/pull/24)

## 2.0.3
  * Fixes schema of `external_reference_id` to be typed as strings [#23](https://github.com/singer-io/tap-harvest/pull/23)

## 2.0.1
  * Fixes issue with empty datetimes [#22](https://github.com/singer-io/tap-harvest/pull/22)
  * Fixes issue with certain datetime fields being treated as dates [#22](https://github.com/singer-io/tap-harvest/pull/22)
  * Merges `*_message_recipients` sub-streams into their parent streams [#22](https://github.com/singer-io/tap-harvest/pull/22)

## 2.0.0
  * Upgrades to Harvest V2 API and modifies streams to match the new endpoints [#20](https://github.com/singer-io/tap-harvest/pull/20)

## 1.1.1
  * Harvest V1 API