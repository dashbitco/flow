# Changelog

## v0.13.0 (2018-01-23)

  * Enhancements
    * Expose a timeout parameter for start_link and into_stages
    * Allow shutdown time for stages to be configured

  * Bug fixes
    * Ensure proper shutdown propagation on start_link, into_stages and friends (#40)
    * Ensure proper shutdown order in Flow (#35)

## v0.12.0

  * Enhancements
    * Allow late subscriptions to Flow returned by `Flow.into_stages`

  * Bug fixes
    * Cancel timer when termination is triggered on periodic window. This avoid invoking termination callbacks twice.

## v0.11.1

  * Enhancements
    * Add the ability to emit only certain events in a trigger

  * Bug fixes
    * Add `:gen_stage` to the applications list
    * Ensure we handle supervisor exits on flow coordinator
    * Ensure we do not unecessary partition when fusing producer+streams

## v0.11.0

Extracted from GenStage.
