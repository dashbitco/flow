# Changelog

## v0.14.0-dev

Flow is inspired on three technologies:

  * GenStage - built on Elixir and provides the foundation for Flow
  * Apache Spark - the inspiration for most of the API in the `Flow` module
  * Apache BEAM - the model we used for windows and triggers seen in `Flow.Window`

Prior to this version, the `Flow` module was responsible for traversing events in the mapper stage and to accumulate the state in reducing stages. When working with unbound data, the `Flow.Window` was used to control exactly when to emit data from the reducing stages.

This approach meant that understanding what happens with a partition state was hard to understand, as it was spread over the flow and window definitions. To make matters worse, if you wanted to have your own rules for emitting events, such as user session or sliding windows, it was only possible to achieve it via custom window implementations.

This design limitation caused many users to drop Flow and use GenStage, as GenStage provides the necessary abstractions for tackling those problems. However, since Flow is built on top of GenStage, why not expose it directly through Flow? That's what v0.14.0 does.

v0.14.0 introduces two new functions: `emit_and_reduce/3` and `on_trigger/2` which gives developers explicit control of when to emit data. The `on_trigger/2` function also allows developers to fully control the state that is kept in the reducing stage after the trigger.

Unfortunately this change is incompatible (or rather, fully replaces) w functionalities:

  * `each_state/2` and `map_state/2` - those two functions were only invoked when there was a trigger and they have now been replaced by a more explicitly named `on_trigger/2` function

  * The `:keep` and `:reset` argument to windows and triggers have been removed as you control the behaviour on `on_trigger/2`

We believe `emit_and_reduce/3` and `on_trigger/2` provide a conceptually simpler module to reason about flows while being more powerful.

This release also deprecates `Flow.Window.session/3` as developers trivially roll their own with more customization power and flexibility using `emit_and_reduce/3` and `on_trigger/2`.

### Summary

  * Enhancements
    * Added `emit_and_reduce/3` and `on_trigger/2`

  * Deprecations
    * Session windows are deprecated in favor of `Flow.emit_and_reduce/3` and `Flow.on_trigger/2`

  * Backwards incompatible changes
    * `Flow.map_state/2` was removed in favor of `Flow.on_trigger/2`
    * `Flow.each_state/2` was removed in favor of `Flow.on_trigger/2`
    * Passing `:keep` or `:reset` to triggers was removed in favor of explicit control via `Flow.on_trigger/2`. If you are passing or matching on those atoms, those entries can be removed

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
