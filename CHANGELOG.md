# Changelog

## v1.2.4 (2023-03-21)

  * Bug fixes
    * Make sure flows are garbage collected when consumed as a stream which is halted

## v1.2.3 (2023-01-07)

  * Enhancements
    * Add `Flow.stream/2` and `Flow.run/2` which accept `link: false`

## v1.2.2 (2023-01-06)

  * Bug fix
    * Fix `Flow.merge/2` followed by consumer

## v1.2.1 (2022-12-29)

  * Enhancements
    * Allow stages to be ignored
    * Add `Flow.child_spec/1`

## v1.2.0 (2022-02-12)

  * Bug fixes
    * Fix bugs where shuffling would ignore partitions before/after
    * Ensure flow options effectively override producer dispatcher

  * Deprecations
    * Deprecate `Flow.map/2` and friends after `reduce` to avoid confusion regarding bookkeeping of state

## v1.1.0 (2020-12-09)

  * Enhancements
    * Add `Flow.map_batch/2`

  * Bug fixes
    * Do not leak flows while enumerating

## v1.0.0 (2020-02-03)

  * Enhancements
    * Require GenStage v1.0.0

## v0.15.0 (2019-10-28)

  * Enhancements
    * Add on_init callback to MapReducer
    * Deprecate Flow.each to avoid pitfalls
    * Remove previously deprecated code

  * Bug fixes
    * Set demand to accumulate before producer_consumer subscribe

## v0.14.3 (2018-10-25)

  * Bug fixes
    * Don't fuse mappers into enumerables (#62)
    * Trap exits to ensure event completion on shutdown
    * Fix `flat_map` followed by `emit_and_reduce` (#68)

## v0.14.2 (2018-07-24)

  * Bug fixes
    * Make sure consumers added via `into_specs/3` restart the flow in case of failures

## v0.14.1 (2018-07-17)

  * Deprecations
    * `Flow.filter_map/3` is deprecated in favor of filter+map
    * `Flow.from_stage/2` is deprecated in favor of `Flow.from_stages/2`
    * `Flow.merge/2` is deprecated in favor of `Flow.partition/2` or `Flow.shuffle/2`

  * Enhancements
    * Add `Flow.shuffle/2` to shuffle the stages into new ones
    * Add `Flow.through_stages/3` for hooking `producer_consumer`s into the flow
    * Add `Flow.from_specs/2`, `Flow.through_specs/3` and `Flow.into_specs/3` to start stages in the same supervision tree as the flow

## v0.14.0 (2018-06-10)

This release includes a redesign of how triggers and the reducing accumulator works.

Prior to this version, the `Flow` module was responsible for traversing events in the mapper stage and to accumulate the state in reducing stages. When working with unbound data, the `Flow.Window` was used to control exactly when to emit data from the reducing stages and when to reset the partition state.

This approach meant that understanding which data is emitted and when the state was reset was hard because the logic was spread in multiple places. To make matters worse, if you wanted to have your own rules for emitting events, such as user session or sliding windows, it was only possible to achieve it via custom window implementations.

This design limitation caused many users to drop Flow and use GenStage, as GenStage provides the necessary abstractions for tackling those problems. However, since Flow is built on top of GenStage, why not expose it directly through Flow? That's what v0.14.0 does.

v0.14.0 introduces two new functions: `emit_and_reduce/3` and `on_trigger/2` which gives developers explicit control of when to emit data. The `on_trigger/2` function also allows developers to fully control the state that is kept in the reducing stage after the trigger.

Unfortunately this change is incompatible (or rather, fully replaces) the following functionalities:

  * `each_state/2` and `map_state/2` - those two functions were only invoked when there was a trigger and they have now been replaced by a more explicitly named `on_trigger/2` function

  * The `:keep` and `:reset` argument to windows and triggers have been removed as you control the behaviour on `on_trigger/2`

For example, if you used `map_state/2` (or `each_state/2`) and a `:reset` trigger, like this:

    |> Flow.map_state(fn acc -> do_something(acc) end)

You can now replace this code by:

    |> Flow.on_trigger(fn acc -> {do_something(acc), []} end)

Where the first element of the tuple returned by `on_trigger` is the data to emit and the second element is the new accumulator of the reducer stage. Similarly, if you were using `map_state/2` (or `each_state/2`) and a `:keep` trigger, like this:

    |> Flow.map_state(fn acc -> do_something(acc) end)

You can now replace this code by:

    |> Flow.on_trigger(fn acc -> {do_something(acc), acc} end)

Note that `on_trigger/2` can only be called once per partition. In case you were calling `map_state/2` and `each_state/2` multiple times, you can simply inline all calls inside the same `on_trigger/2`.

We believe `emit_and_reduce/3` and `on_trigger/2` provide a conceptually simpler module to reason about flows while being more powerful.

This release also deprecates `Flow.Window.session/3` as developers can trivially roll their own with more customization power and flexibility using `emit_and_reduce/3` and `on_trigger/2`.

### Notes

  * Enhancements
    * `use Flow` now defines a `child_spec/1` to be used under supervision
    * Added `emit_and_reduce/3` and `on_trigger/2`
    * Use `DemandDispatcher` when there is one stage in partition

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
    * Ensure we do not unnecessary partition when fusing producer+streams

## v0.11.0

Extracted from GenStage.
