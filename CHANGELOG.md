# Changelog

## 2.0.5
  * Fixes `expenses` stream to use `expense.creator` instead of `expense.user` [#26](https://github.com/singer-io/tap-harvest/pull/26)

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
