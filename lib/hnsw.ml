open Base
(*  https://arxiv.org/abs/1603.09320  *)

(* 
   I cannot find a priority queue or heap in Base.
   Core_kernel has FHeap and Heap which look like they might work.

   A pairing heap looks fun to implement.
 *)
module type COMPARABLE = sig
  type t [@@deriving sexp]
  val compare : t -> t -> int
end
module PairingHeap(Element : COMPARABLE) = struct
  (* Functional min heap.  *)
  type t =
    | Empty
    | Node of { node : Element.t; subheaps : t list } [@@deriving sexp]

  let create () = Empty
  let singleton x = Node { node = x; subheaps = [] }
  let merge (a : t) (b : t) = match a, b with
    | Empty, _ -> b
    | _, Empty -> a
    | Node { node=node_a; subheaps=subheaps_a },
      Node { node=node_b; subheaps=subheaps_b } ->
      if Element.compare node_a node_b < 0 then Node { node=node_a; subheaps=b::subheaps_a }
      else Node { node=node_b; subheaps=a::subheaps_b }
  let add h x = merge h (singleton x)
  let top = function
    | Empty -> None
    | Node { node; _ } -> Some node

  let rec merge_pairs = function
    | [] -> Empty
    | [h] -> h
    | h1::h2::q ->
      merge (merge h1 h2) (merge_pairs q)

  let remove_top = function
    | Empty -> Empty
    | Node { node; subheaps } -> merge_pairs subheaps

  let pop_top = function
    | Empty -> None
    | Node { node; subheaps } -> Some (node, merge_pairs subheaps)

  (*  order unspecified  *)
  let rec fold h ~init ~f = match h with
    | Empty -> init
    | Node { node; subheaps } ->
      List.fold_left subheaps ~init:(f init node) ~f:(fun acc h -> fold h ~init:acc ~f)

  type 'a continue =
    | Stop of 'a
    | Continue of 'a
  let rec fold_near_to_far_until h ~init ~f = match h with
    | Empty -> init
    | Node { node; subheaps } ->
      match f init node with
      | Stop result -> result
      | Continue result -> fold_near_to_far_until (merge_pairs subheaps) ~init:result ~f
end

(* module type GRAPH_HEAP = sig *)
(*   type t *)
(*   type node [@@deriving sexp] *)
(*   type value *)
(*   val distance : value -> value -> float *)
(*   val value : t -> node -> value *)
(* end *)
module type DISTANCE = sig
  type value
  val distance : value -> value -> float
end
module type VALUE = sig
  type t
  type node [@@deriving sexp]
  type value
  val value : t -> node -> value
end
module MinHeap(Distance : DISTANCE)(Value : VALUE with type value = Distance.value) = struct
  module Element = struct
    type t = { distance_to_target : float; node : Value.node } [@@deriving sexp]

    let compare a b = Float.compare a.distance_to_target b.distance_to_target
    let of_node target g node = {
      distance_to_target=Distance.distance target (Value.value g node);
      node=node
    }
    let is_nearer a b = compare a b < 0
  end

  include PairingHeap(Element)
  let min = top
  let remove_min = remove_top
  let pop_min = pop_top
  let of_fold target g f =
    f ~init:(create ()) ~f:(fun acc node -> add acc (Element.of_node target g node))

  let nearest_k k target g node_fold =
    let rev, _ = fold_near_to_far_until (of_fold target g node_fold) ~init:([], 0)
        ~f:(fun ((acc, n) as accn) e -> if n = k then Stop accn else Continue (e::acc, n+1))
    in List.rev rev

  let nearest target g node_fold =
    min (of_fold target g node_fold)
end

(*  XXX too much copy-paste between this and MinHeap? *)
module MaxHeap(Distance : DISTANCE)(Value : VALUE with type value = Distance.value) = struct
  module Element = struct
    type t = { distance_to_target : float; node : Value.node } [@@deriving sexp]

    (*  order reversed to get a max heap  *)
    let compare a b = Float.compare b.distance_to_target a.distance_to_target
    let of_node target g node = {
      distance_to_target=Distance.distance target (Value.value g node);
      node=node
    }
    let is_further a b = compare a b < 0
  end

  include PairingHeap(Element)
  let max = top
  let remove_max = remove_top
  let pop_max = pop_top
  let of_fold target g f =
    f ~init:(create ()) ~f:(fun acc node -> add acc (Element.of_node target g node))
end

module type NEAREST = sig
  (* This should be implemented in a way that insert is fast
     (probably as max priority queue).
  *)
  type t_value_computer
  type node
  type value
  type t
  val create : t_value_computer -> value -> int -> t
  type insert = Too_far | Inserted of t
  val insert : t -> node -> insert
  (* val of_fold : (init:'acc -> f:('acc -> node -> 'acc) -> 'acc) -> t *)
  val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
end

module type VISIT_ME = sig
  type t_value_computer
  type nearest
  type node
  type value
  type t
  val singleton : t_value_computer -> value -> node -> t
  val add : t -> node -> t
  val pop_nearest : t -> (node * t) option
  val nearest : t -> node option
  val of_nearest : nearest -> t
end

module type SEARCH_GRAPH = sig
  type t [@@deriving sexp]
  type node [@@deriving sexp]

  module Visited : sig
    type t
    val create : unit -> t
    type visit = Already_visited | New_visit of t
    val visit : t -> node -> visit
  end

  module Neighbours : sig
    type t
    type nonrec node = node
    val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
  end

  val adjacent : t -> node -> Neighbours.t
end

module Search
    (Graph : SEARCH_GRAPH)
    (VisitMe : VISIT_ME with type node = Graph.node)
    (Nearest : NEAREST with type node = Graph.node and type value = VisitMe.value)
    (Distance : DISTANCE with type value = VisitMe.value)
    (Value : VALUE with type value = Distance.value
                    and type node = Graph.node
                    and type t = Nearest.t_value_computer) =
struct
  module MinHeap = MinHeap(Distance)(Value)

  (* Searching nearest neighbours in a single knn graph. Greedy search
     with a limited backtracking set.

     This can be used as a building block of both HNSW and SNG.

     This is expressed as in the NSG paper (this is a different
     (probably equivalent? not completely sure) formulation than in
     the HNSW one)

     Here we do not have the shortcut that the best node to visit is
     worse than the worst nearest node. Does it matter? I think
     not.  *)
  let search
      (hgraph : Value.t)
      (graph : Graph.t)
      (start_nodes : VisitMe.t)
      (target : Value.value)
      (size_nearest : int) =
    let rec aux visit_me visited nearest =
      (*  notes:
          - nodes in visit_me are possibly not already in visited
          - in the usage here, Nearest should be a fixed-size
          max-heap: inserting in the heap when it has reached max size
          should either do nothing (the inserted is worse than the
          existing max), or evict the previous max.
      *)
      match VisitMe.pop_nearest visit_me with
      | None -> nearest
      | Some (visit_node, visit_me) ->
        match Graph.Visited.visit visited visit_node with
        | Already_visited -> aux visit_me visited nearest
        | New_visit visited ->
          match Nearest.insert nearest visit_node with
          | Too_far -> aux visit_me visited nearest
          | Inserted nearest ->
            let visit_me =
              Graph.Neighbours.fold (Graph.adjacent graph visit_node) ~init:visit_me ~f:VisitMe.add
            in
            aux visit_me visited nearest
    in
    (* let visit_me = Graph.VisitMe.singleton start_node in *)
    let visited = Graph.Visited.create () in
    let nearest = Nearest.create hgraph target size_nearest in
    aux start_nodes visited nearest

  let search_one hgraph graph start_nodes target size_nearest =
    MinHeap.nearest target hgraph (Nearest.fold (search hgraph graph start_nodes target size_nearest))
end

module type HGRAPH = sig
  type t
  type node [@@deriving sexp]
  type value [@@deriving sexp]

  module LayerGraph : sig
    (* This must satisfy SEARCH_GRAPH and NEIGHBOUR_GRAPH.  *)
    type t [@@deriving sexp]
    type nonrec node = node [@@deriving sexp]
    type nonrec value = value [@@deriving sexp]

    module Visited : sig
      type t
      val create : unit -> t
      type visit = Already_visited | New_visit of t
      val visit : t -> node -> visit
    end

    module Neighbours : sig
      type t
      type nonrec node = node
      val create : unit -> t
      val add : t -> node -> t
      val length : t -> int
      val for_all : t -> f:(node -> bool) -> bool
      val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
    end

    val adjacent : t -> node -> Neighbours.t
  end

  val is_empty : t -> bool

  val value : t -> node -> value
  (* val distance : value -> value -> float *)
  
  val create : unit -> t
  val layer : t -> int -> LayerGraph.t

  val max_layer : t -> int
  val set_max_layer : t -> int -> t

  val entry_point : t -> node
  val set_entry_point : t -> node -> t

  (*  add a new node with a given value, and given neighbours  *)
  val insert : t -> int -> value -> LayerGraph.Neighbours.t -> t

  (*  overwrite connections of a node  *)
  val set_connections : t -> int -> node -> LayerGraph.Neighbours.t -> t
end

module type NEIGHBOUR_GRAPH = sig
  type t [@@deriving sexp]
  type node [@@deriving sexp]

  module Neighbours : sig
    type t
    val create : unit -> t
    val add : t -> node -> t
    val length : t -> int
    val for_all : t -> f:(node -> bool) -> bool
  end
end

module SelectNeighbours
    (Graph : NEIGHBOUR_GRAPH)
    (Distance : DISTANCE)
    (Value : VALUE with type value = Distance.value and type node = Graph.node) = struct
  module MinHeap = MinHeap(Distance)(Value)
  (*  
SELECT-NEIGHBORS-HEURISTIC(hnsw, bEl, cQueue, M, l, extendCandidates,
                           keepPrunedConnections)
1  resSet = {}
2  wcQueue = cQueue // MIN-PRIORITY-QUEUE,
                    // key – distance to bEl
3  if extendCandidates
4    for each e ∈ cQueue
5      for each e1 ∈ hnsw.adj(e, l)
6        if e1 ∉ wcQueue
7          INSERT(wcQueue, e1)
8  wSet = {}
9  while wcQueue.size > 0
10   e = EXTRACT-MIN(wcQueue)
11   if e is closer to bEl compared to any element from resSet
12     resSet = resSet ∪ e
13   else
14     wSet = wSet ∪ e
15   if resSet.size > M
16     break
17 if keepPrunedConnections // add some of the discarded
                            // connections
18   wQueue = MIN-PRIORITY-QUEUE(wSet) // key – distance to bEl
19   while wQueue.size > 0
20     resSet = resSet ∪ EXTRACT-MIN(wQueue)
21     if resSet.size >= M
22       break
23 return resSet

Note: not implementing extendCandidates and keepPrunedConnections for
now, as the paper says they are off by default and I am unclear on how
useful they are.

  *)

  let select_neighbours
      hgraph (g : Graph.t) target
      ~value ~distance
      (nearest_fold : init:'acc -> f:('acc -> Graph.node -> 'acc) -> 'acc)
      (num_neighbours : int) : Graph.Neighbours.t =
    let is_closer target (node : MinHeap.Element.t) neighbours =
      let node_value = value node.node in
      let distance_target_node = node.distance_to_target in
      Graph.Neighbours.for_all neighbours ~f:(fun neighbour ->
          Float.(>) (distance node_value (value neighbour)) distance_target_node
        )
    in
    let min_heap = MinHeap.of_fold target hgraph nearest_fold in
    MinHeap.fold_near_to_far_until min_heap ~init:(Graph.Neighbours.create ()) ~f:(fun neighbours node ->
        if is_closer target node neighbours then begin
          let neighbours = Graph.Neighbours.add neighbours node.node in
          if Graph.Neighbours.length neighbours >= num_neighbours then MinHeap.Stop neighbours
          else MinHeap.Continue neighbours
        end else MinHeap.Continue neighbours
      )
end

module Build
    (Hgraph : HGRAPH)
    (VisitMe : VISIT_ME with type node = Hgraph.node
                         and type value = Hgraph.value
                         and type t_value_computer = Hgraph.t)
    (Nearest : NEAREST with type node = Hgraph.node
                        and type value = Hgraph.value
                        and type t_value_computer = Hgraph.t
                        and type t = VisitMe.nearest)
    (Distance : DISTANCE with type value = Hgraph.value) = struct
  module Layer = Hgraph.LayerGraph
  module SearchLayer = Search(Layer)(VisitMe)(Nearest)(Distance)(Hgraph)
  module SelectNeighbours = SelectNeighbours(Layer)(Distance)(Hgraph)

  (*
  INSERT(hnsw, q, M, efConstruction, levelMult)
1  wQueue = NIL // MIN-PRIORITY-QUEUE
                // key – distance to q
2  visSet = {}
3  epSet = {hnsw.entPoint}
4  efOne = 1
5  level = ⌊-ln(RANDOM(0..1))• levelMult⌋ // Select a random level
                                         // with an exponential
                                         // distribution
6  for l = hnsw.maxLayer downto level+1
7    wQueue = SEARCH-LEVEL(hnsw, q, epSet, efOne, l)
8    epSet = {MIN(wQueue)}
9  for l = min(hnsw.maxLayer, level) downto 0
10   wQueue = SEARCH-LEVEL(hnsw, q, epSet, efConstruction, l)
11   neighborsSet = SELECT-NEIGHBORS(q, wQueue, M, l)
                                        // alg. 3 or alg. 4
12   bidirectionally connect elements from neighborsSet to q
13   for each e ∈ neighborsSet // shrink lists of connections
14     eConnSet = hnsw.adj(e, l)
15     if eConnSet.size > hnsw.Mmax
16       eConnQueue = MIN-PRIORITY-QUEUE(eConnSet)
17       eNewConnSet = SELECT-NEIGHBORS(e, eConnQueue,
                                        hnsw.maxM, l) // alg. 3 or alg. 4
18       hnsw.adj(e, l) = eNewConnSet
19   epSet = SET(wQueue)
20 if level > hnsw.maxLayer // update hnsw.entPoint
21   hnsw.maxLayer = level
22   hnsw.entPoint = q

 *)
  let insert hgraph point ~num_neighbours ~max_num_neighbours ~num_neighbours_search ~level_mult =
    if Hgraph.is_empty hgraph then Hgraph.insert hgraph 0 point (Layer.Neighbours.create ()) else
      let level = Int.of_float @@ Float.round_nearest @@ -. (Float.log (Random.float 1.)) *. level_mult in
      let rec aux (hgraph : Hgraph.t) (i_layer : int) (start_nodes : VisitMe.t) =
        if i_layer < 0 then hgraph, start_nodes else begin
          let layer_graph = Hgraph.layer hgraph i_layer in
          if i_layer > level then begin
            match SearchLayer.search_one hgraph layer_graph start_nodes point num_neighbours_search with
            | None -> failwith "unexpected: found no neighbour at all"
            | Some nearest -> aux hgraph (i_layer - 1)
                                (VisitMe.singleton hgraph point nearest.node)
          end else begin
            let nearest = SearchLayer.search hgraph layer_graph start_nodes point num_neighbours_search in
            let neighbours = SelectNeighbours.select_neighbours
                hgraph
                ~value:(Hgraph.value hgraph)
                ~distance:Distance.distance
                layer_graph point (Nearest.fold nearest) num_neighbours
            in
            let hgraph = Hgraph.insert hgraph i_layer point neighbours in
            let hgraph = Layer.Neighbours.fold neighbours ~init:hgraph
                ~f:(fun (hgraph : Hgraph.t) (node : Layer.node) ->
                    (* recompute this since we changed hgraph above  *)
                    let layer_graph = Hgraph.layer hgraph i_layer in
                    let connections = Layer.adjacent layer_graph node in
                    if Layer.Neighbours.length connections > max_num_neighbours then begin
                      let new_connections =
                        SelectNeighbours.select_neighbours hgraph layer_graph
                          ~value:(Hgraph.value hgraph)
                          ~distance:Distance.distance
                          (Hgraph.value hgraph node)
                          (Layer.Neighbours.fold connections)
                          max_num_neighbours
                      in
                      Hgraph.set_connections hgraph i_layer node new_connections
                    end else hgraph
                  )
            in aux hgraph (i_layer - 1) (VisitMe.of_nearest nearest)
          end
        end
      in
      let hgraph, nearest =
        aux hgraph (Hgraph.max_layer hgraph)
          (VisitMe.singleton hgraph point (Hgraph.entry_point hgraph))
      in
      (* We use max_layer/set_max_layer to notice that we have to update
         the entry point. This is not satisfying because the graph could
         manage its max layer by itself, since it knows about it. *)
      if level > (Hgraph.max_layer hgraph) then
        let point_node = match VisitMe.nearest nearest with
          | Some node -> node
          | None -> failwith "unexpected: visitme set is empty!"
        in
        Hgraph.set_max_layer (Hgraph.set_entry_point hgraph point_node) level
      else hgraph

  let create point_fold ~num_neighbours ~max_num_neighbours ~num_neighbours_search ~level_mult =
    point_fold ~init:(Hgraph.create ()) ~f:(fun hgraph point ->
        insert hgraph point ~num_neighbours ~max_num_neighbours ~num_neighbours_search ~level_mult)
end

module type KNN_HGRAPH = sig
  type t
  type value
  type node [@@deriving sexp]
  module LayerGraph : sig
    include SEARCH_GRAPH with type node = node
    (* val distance : value -> value -> float *)
    (* val value : t -> node -> value *)
  end

  val layer : t -> int -> LayerGraph.t
  val entry_point : t -> node
  val max_layer : t -> int
  val value : t -> node -> value
end

module Knn
    (Hgraph : KNN_HGRAPH)
    (VisitMe : VISIT_ME with type node = Hgraph.node
                         and type value = Hgraph.value
                         and type t_value_computer = Hgraph.t)
    (Nearest : NEAREST with type node = Hgraph.node
                        and type value = VisitMe.value
                        and type t_value_computer = Hgraph.t)
    (Distance : DISTANCE with type value = Hgraph.value) = struct
  (* K-NN-SEARCH(hnsw, q, K, ef)
   * 1 wQueue = NIL
   * 2 epSet = {hnsw.entPoint}
   * 3 efOne = 1
   * 4 levelZero = 0
   * 5 for l = hnsw.maxLayer downto 1
   * 6   wQueue = SEARCH-LEVEL(hnsw, q, epSet, efOne, l)
   * 7   epSet = {MIN (wQueue)}
   * 8 wQueue = SEARCH-LEVEL(hnsw, q, epSet, ef, levelZero)
   * 9 resSet = {}
   * 10 for i = 0 to K-1
   * 11   resSet = resSet ∪ EXTRACT-MIN(wQueue)
   * 12 return resSet

     notes:
     - wqueue is not always needed if we replace SEARCH-LEVEL by SEARCH-LEVEL-MIN
  *)

  module Search = Search(Hgraph.LayerGraph)(VisitMe)(Nearest)(Distance)(Hgraph)
  module MinHeap = MinHeap(Distance)(Hgraph)

  let knn hgraph target ?(num_additional_neighbours_search=0) ~num_neighbours =
    let rec aux start_nodes i_layer =
      let layer = Hgraph.layer hgraph i_layer in
      if i_layer <= 0 then
        let neighbours =
          Search.search hgraph layer start_nodes target (num_neighbours + num_additional_neighbours_search)
        in
        MinHeap.nearest_k num_neighbours target hgraph (Nearest.fold neighbours)
      else
        match Search.search_one hgraph layer start_nodes target 1 with
        | None -> failwith "unexpected: found no neighbour at all"
        | Some nearest ->
          aux
            (VisitMe.singleton hgraph target nearest.node)
            (i_layer - 1)
    in
    aux
      (VisitMe.singleton hgraph target (Hgraph.entry_point hgraph))
      (Hgraph.max_layer hgraph)
end

