{
  lib,
  fx,
  ...
}:
let
  # ---------------------------------------------------------------------------
  # run :: drivers -> cycle-c -> sinks
  #
  # Cycle.js fixed-point: each source is its driver applied to the matching
  # sink. Nix laziness makes the mutual recursion safe.
  # ---------------------------------------------------------------------------
  run =
    drivers: cycle-c:
    let
      sources = builtins.mapAttrs (name: drv-d: drv-d sinks.${name}) drivers;
      sinks = cycle-c sources;
    in
    sinks;

  # ---------------------------------------------------------------------------
  # ctx-s :: contextual-fn -> st
  #
  # Executes fn after effect-requests for each of its named arguments.
  # Produces a singleton st from fn result. See fx.bind.fn and fx.rotate.
  # ---------------------------------------------------------------------------
  ctx-s = f: wrap (fx.stream.more (fx.bind.fn { } f) (fx.stream.done null));

  # ---------------------------------------------------------------------------
  # ctx-d :: bindings -> comp-s -> comp-s
  #
  # Provides constant context values to a stream of contextual computations.
  # Each computation in comp-s is run with bindings as named effects.
  # ---------------------------------------------------------------------------
  ctx-d = bindings: scope-d (fx.effects.scope.handlersFromAttrs bindings);

  # ---------------------------------------------------------------------------
  # static-d :: [a] -> request -> ST a
  #
  # Driver that emits a fixed list of items, ignoring requests.
  # Canonical way to turn static data into a source stream.
  static-d = items: _: st.fromList items;

  # collect-d :: ST a -> ST [ a ]
  #
  # Driver that collects all incoming items and produces a singleton
  # stream with all those items as a list.
  # Useful to use a sink as collector and read source of all values.
  collect-d = items: st items.toList;

  # map-c :: (a -> b) -> ST a -> ST b
  #
  # Cycle constructor for pure functions. Wraps f as a stream transformer.
  # Both host-c and user-c in fleet-demo are instances of this pattern.
  map-c = f: stream: stream (st.flatMap (x: st (f x)));

  # when-c :: (a -> bool) -> ST a -> (a -> b) -> ST b
  #
  # Conditional stream transformer — Ned equivalent of Den's policy.when.
  # Filters stream to items matching pred, then maps f over survivors.
  #
  # Den:  policy.when (ctx: ctx.host.role == "lb") (policy.provide {class="nixos"; module=M})
  # Ned:  when-c (h: h.role == "lb") host-stream  (h: {hostName=h.name; module=M})
  #
  # The predicate plays the role of Den's guard: it receives one stream
  # item (the "scope" in Den's terms) and returns bool. If false, item
  # is dropped — equivalent to Den deferring a guard that doesn't pass.
  when-c =
    pred: stream: f:
    (stream (st.filter pred)) (st.map f);

  # fanout-d :: stream -> ctx-fn -> comp-s -> st
  #
  # Fanout pattern: for each item in stream, derive context and apply comp-s.
  # ctx-fn: item -> context-bindings
  # Commonly used for iterating collections with context injection.
  # Point-free: stream is data (first), comp-s is final composition (last).
  # ---------------------------------------------------------------------------
  fanout-d =
    stream: ctx-fn: comp-s:
    stream.flatMap (item: (ctx-d (ctx-fn item) comp-s));

  # ---------------------------------------------------------------------------
  # scope-d :: handlers -> compS -> compS
  #
  # Provides effect handlers to a stream of contextual computations.
  # Each computation in compS is run with scoped as effect handler (stateless)
  # ---------------------------------------------------------------------------
  scope-d =
    handlers: comp-s:
    let
      rot = fx.rotate {
        inherit handlers;
        return = value: _state: value;
      };
      walk-rot =
        stream:
        fx.bind (rot stream) (
          step:
          if step._tag == "Done" then
            fx.stream.done step.value
          else
            fx.stream.more step.head (walk-rot step.tail)
        );
      go =
        stream:
        fx.bind (rot stream) (
          step:
          if step._tag == "Done" then
            fx.stream.done step.value
          else
            let
              comp = if fx.isComp step.head then step.head else fx.pure step.head;
            in
            fx.bind (rot comp) (
              val:
              let
                inner = if val ? __stream then val.__stream else fx.stream.more (val) (fx.stream.done null);
              in
              fx.stream.concat (walk-rot inner) (go step.tail)
            )
        );
    in
    wrap (go comp-s.__stream);

  # Empty ST — identity for concat, curried ST.* functions
  st = {
    __stream = fx.stream.done null;
    __functor = (wrap st.__stream).__functor;
    fromList = list: wrap (fx.stream.fromList list);
    fromNames = attrs: st (builtins.attrNames attrs);
    fromValues = attrs: st (builtins.attrValues attrs);
    forList = xs: f: (st.fromList xs) (st.flatMap f);
    # filterList :: (a -> bool) -> [a] -> (a -> ST b) -> ST b
    # Filter xs by pred, then fan out via f. Composes filter + forList.
    filterList =
      pred: xs: f:
      st.forList (builtins.filter pred xs) f;
    # select :: (a -> bool) -> ST a -> [a]
    # Filter stream to list in one step. Common collect-by-predicate pattern.
    select = pred: stream: (stream (st.filter pred)).toList;
    # withPeers :: (a -> key) -> ([a] -> a -> ST b) -> ST a -> ST b
    # Group source by key-fn; iterate each item with its group-peers in scope.
    # Core mechanism for env-scoped pipes: siblings see each other's data.
    # Used as juxtaposition combinator: source (st.withPeers key-fn f)
    withPeers =
      key-fn: f: source-st:
      let
        by-key = lib.groupBy key-fn source-st.toList;
      in
      st.forList (builtins.attrValues by-key) (peers: st.forList peers (f peers));
    toList = [ ];
    wrap = wrap;
    map = f: self: self.map f;
    # Curried refinement via st.focus (no parent reference metadata)
    filter = p: self: self.filter p;
    flatMap = f: self: self.flatMap f;
    scanl =
      f: z: self:
      self.scanl f z;
    concat = s: self: self.concat s;

    # dedup :: (a -> key) -> ST a -> ST a
    # Drop items whose key was already seen. Keeps first occurrence.
    # Uses scanl to track seen keys; flatMap emits item or nothing.
    dedup =
      key-fn: stream:
      let
        step =
          state: item:
          let
            k = key-fn item;
          in
          if builtins.elem k state.seen then
            {
              seen = state.seen;
              emit = [ ];
            }
          else
            {
              seen = state.seen ++ [ k ];
              emit = [ item ];
            };
        states = stream (
          st.scanl step {
            seen = [ ];
            emit = [ ];
          }
        );
      in
      states (st.flatMap (s: st.fromList s.emit));

    # flatten :: ST (ST a) -> ST a
    # Merge stream-of-streams into one stream. Generic fan-in.
    flatten = stream-of-streams: stream-of-streams (st.flatMap (s: s));

    # ---------------------------------------------------------------------------
    # Lens-ready operations — library-agnostic, work with any { get, set } lens.
    # ---------------------------------------------------------------------------

    # get :: lens-or-predicate -> ST -> ST
    # lens { get, set }: maps lens.get over each element → stream of Either.
    # predicate fn: filters stream (same as st.filter).
    # Works with any lens library following the { get, set } protocol.
    get =
      f: self:
      let
        isLens =
          builtins.isAttrs f
          && f ? get
          && f ? set
          && builtins.isFunction f.get
          && builtins.isFunction f.set
          && !(f ? __stream)
          && !(f ? toList);
      in
      if isLens then self f else self.filter f;

    # set :: lens -> value -> ST -> ST
    # For each element s: lens.set s value — works with any { get, set } lens.
    set =
      lens: value: self:
      self.map (s: lens.set s value);
  };

  # ---------------------------------------------------------------------------
  # wrap :: fx.stream -> ST
  #
  # Wraps a raw fx.stream as an ST with a fluent functor API.
  # Calling the result as a functor chains additional values:
  #
  #   st other-ST          → concat
  #   st (s: ...)          → stream combinator  (lib.functionArgs = {}, s = self)
  #   st ({ host }: ...)   → contextual fn      (lib.functionArgs non-empty)
  #   st plain-value       → emit as single element
  #
  # flatMap accepts f returning either a raw fx.stream or an ST —
  # callers never need to touch .__stream.
  # ---------------------------------------------------------------------------
  wrap =
    raw-stream:
    if raw-stream ? __stream then
      raw-stream
    else
      let
        unwrap-st = s: if s ? __stream then s.__stream else fx.stream.more (s) (fx.stream.done null);

        sub-filter-map =
          name: mapper: wrap (fx.stream.map mapper (fx.stream.filter (x: x ? ${name}) raw-stream));

        # Protocol detection: any { get, set } lens — library-agnostic.
        isLens =
          v:
          builtins.isAttrs v
          && v ? get
          && v ? set
          && builtins.isFunction v.get
          && builtins.isFunction v.set
          && !(v ? __stream)
          && !(v ? toList);

        # Apply lens.get to each stream element → stream of Either.
        applyLensToStream = lens: wrap (fx.stream.map (x: lens.get x) raw-stream);

        self = {
          __stream = raw-stream;

          __functor =
            _: v:
            if isLens v then
              applyLensToStream v
            else if v ? __stream then
              wrap (fx.stream.concat raw-stream v.__stream)
            else if builtins.isFunction v then
              (if lib.functionArgs v == { } then v self else self (ctx-s v))
            else if builtins.isList v then
              wrap (fx.stream.concat raw-stream (fx.stream.more (v) (fx.stream.done null)))
            else
              wrap (fx.stream.concat raw-stream (fx.stream.more (v) (fx.stream.done null)));

          map = f: wrap (fx.stream.map f raw-stream);
          filter = p: wrap (fx.stream.filter p raw-stream);
          concat = other-s: wrap (fx.stream.concat raw-stream other-s.__stream);

          flatMap = f: wrap (fx.stream.flatMap (x: unwrap-st (f x)) raw-stream);
          scanl = f: z: wrap (fx.stream.scanl f z raw-stream);

          fields = {
            value = name: sub-filter-map name (x: x.${name});
            apply = name: value: sub-filter-map name (x: x.${name} value);
            __functor = _: name: self.flatMap (attrs: if attrs ? ${name} then attrs.${name} else st);
          };

          # Bend as combinator support
          right = wrap (fx.stream.filter (x: x ? right) raw-stream);
          left = wrap (fx.stream.filter (x: x ? left) raw-stream);

          toList = (fx.handle { handlers = { }; } (fx.stream.toList raw-stream)).value;
        };
      in
      self;
in
{
  inherit
    collect-d
    ctx-d
    ctx-s
    fanout-d
    map-c
    run
    scope-d
    st
    static-d
    when-c
    ;
}
