# Changelog

## [2.3.0](https://github.com/gmc-norr/st2-scout/compare/v2.2.0...v2.3.0) (2026-10-21)


### Features

* use full igene panel name in scout ([#37](https://github.com/gmc-norr/st2-scout/issues/37)) ([7b26b9b](https://github.com/gmc-norr/st2-scout/commit/7b26b9b99d0502f34991ae95f10d769a70e04618))

## [2.2.0](https://github.com/gmc-norr/st2-scout/compare/v2.1.2...v2.2.0) (2026-10-16)


### Features

* add action `export_case` ([#35](https://github.com/gmc-norr/st2-scout/issues/35)) ([5d0dc0d](https://github.com/gmc-norr/st2-scout/commit/5d0dc0de0cbb8f0fc2481f22832f5fff60f8141d))
* add action `view_case` ([#34](https://github.com/gmc-norr/st2-scout/issues/34)) ([e7df1e9](https://github.com/gmc-norr/st2-scout/commit/e7df1e97394ddac92fb127f43fb89b27f37aea26))
* add action to load gene panel from iGene ([#27](https://github.com/gmc-norr/st2-scout/issues/27)) ([cf757da](https://github.com/gmc-norr/st2-scout/commit/cf757dabe1f26137062ff9f895a13b7aba6b6197))
* compare panels ([#31](https://github.com/gmc-norr/st2-scout/issues/31)) ([e018848](https://github.com/gmc-norr/st2-scout/commit/e018848787958f5cdc2b9dfb185f2dbbb69eb1a8))


### Bug Fixes

* use processed sample ids for scout case names (with fallbacks) ([#29](https://github.com/gmc-norr/st2-scout/issues/29)) ([c5dca7e](https://github.com/gmc-norr/st2-scout/commit/c5dca7efe5166374b746030ed3ca56c2768308c7))
* yaql Get bug fix ([#38](https://github.com/gmc-norr/st2-scout/issues/38)) ([bb1cadf](https://github.com/gmc-norr/st2-scout/commit/bb1cadf6684f52cefffaa50ffd736c5a9f3a4bce))

## [2.1.2](https://github.com/gmc-norr/st2-scout/compare/v2.1.1...v2.1.2) (2026-09-07)


### Bug Fixes

* ignore VerifKonst_PAN_GMS560 panel ([#25](https://github.com/gmc-norr/st2-scout/issues/25)) ([47f0040](https://github.com/gmc-norr/st2-scout/commit/47f00400b930f9c22acf4481ba804ef3df32e6f2))

## [2.1.1](https://github.com/gmc-norr/st2-scout/compare/v2.1.0...v2.1.1) (2026-09-07)


### Bug Fixes

* ignore SNV_konst_GMS560 panel ([#23](https://github.com/gmc-norr/st2-scout/issues/23)) ([2ea6ee4](https://github.com/gmc-norr/st2-scout/commit/2ea6ee48c53272839b3b30d3466a4a99cc0bb26d))
* more failure checks in `generate and load case` workflow ([#22](https://github.com/gmc-norr/st2-scout/issues/22)) ([d15b018](https://github.com/gmc-norr/st2-scout/commit/d15b018808fa419e53bd478b825e70241ff42442))

## [2.1.0](https://github.com/gmc-norr/st2-scout/compare/v2.0.3...v2.1.0) (2026-08-27)


### Features

* use sample_name and ExternalRID for a sample as a unique case_name in scout. For gms-solid ([#20](https://github.com/gmc-norr/st2-scout/issues/20)) ([d0056ab](https://github.com/gmc-norr/st2-scout/commit/d0056abf182842b531d22db48311a467e0538e1e))

## [2.0.3](https://github.com/gmc-norr/st2-scout/compare/v2.0.2...v2.0.3) (2026-08-27)


### Bug Fixes

* fix error message ([0722e7c](https://github.com/gmc-norr/st2-scout/commit/0722e7c4bbca577801c3257f7b687e5ba1db36c9))
* report result() as error message for load_case ([#18](https://github.com/gmc-norr/st2-scout/issues/18)) ([0722e7c](https://github.com/gmc-norr/st2-scout/commit/0722e7c4bbca577801c3257f7b687e5ba1db36c9))

## [2.0.2](https://github.com/gmc-norr/st2-scout/compare/v2.0.1...v2.0.2) (2026-08-26)


### Bug Fixes

* remove empty action files ([#16](https://github.com/gmc-norr/st2-scout/issues/16)) ([15d9c70](https://github.com/gmc-norr/st2-scout/commit/15d9c709f79aecdd58f85b9f91c306d216fb4524))
* remove empty files ([15d9c70](https://github.com/gmc-norr/st2-scout/commit/15d9c709f79aecdd58f85b9f91c306d216fb4524))

## [2.0.1](https://github.com/gmc-norr/st2-scout/compare/v2.0.0...v2.0.1) (2026-08-26)


### Bug Fixes

* update readme ([b14f9ec](https://github.com/gmc-norr/st2-scout/commit/b14f9ecae641ff2fafe6efde67863e00af583cf1))
* update readme ([#14](https://github.com/gmc-norr/st2-scout/issues/14)) ([b14f9ec](https://github.com/gmc-norr/st2-scout/commit/b14f9ecae641ff2fafe6efde67863e00af583cf1))

## [2.0.0](https://github.com/gmc-norr/st2-scout/compare/v1.0.0...v2.0.0) (2026-08-21)


### ⚠ BREAKING CHANGES

* Support for gms_solid analysis ([#11](https://github.com/gmc-norr/st2-scout/issues/11))

### Features

* Support for gms_solid analysis ([#11](https://github.com/gmc-norr/st2-scout/issues/11)) ([13ad6ed](https://github.com/gmc-norr/st2-scout/commit/13ad6ed06dd996cd8f08a441cc7cbfe81a295512))

## 1.0.0 (2026-05-08)


### Features

* update the pack name to scout ([#4](https://github.com/gmc-norr/st2-scout/issues/4)) ([725a120](https://github.com/gmc-norr/st2-scout/commit/725a120660f99729ee568ad3736d6f26dff1b994))
* workflow and actions for loading a case into scout and loqusdb ([#6](https://github.com/gmc-norr/st2-scout/issues/6)) ([6d9517f](https://github.com/gmc-norr/st2-scout/commit/6d9517f1041099776bac66246ee88c2b16cd7c2c))
