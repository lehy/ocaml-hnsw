open Base
open Stdio (* XXX DEBUG  *)
(*  https://arxiv.org/abs/1603.09320  *)

let tsh f x = Sexp.to_string_hum (f x)

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
  let rec fold_top_to_bottom_until h ~init ~f = match h with
    | Empty -> init
    | Node { node; subheaps } ->
      match f init node with
      | Stop result -> result
      | Continue result -> fold_top_to_bottom_until (merge_pairs subheaps) ~init:result ~f
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
  type value [@@deriving sexp]
  val value : t -> node -> value
end
type 'node value_distance = { node : 'node; distance_to_target : float } [@@deriving sexp]

module MinHeap(Distance : DISTANCE)(Value : VALUE with type value = Distance.value) = struct
  module Element = struct
    type t = Value.node value_distance [@@deriving sexp]

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

  let of_fold_distance target g f =
    f ~init:(create ()) ~f:add (* (fun acc node -> add acc (Element.of_node target g node)) *)
  
  let fold_near_to_far_until = fold_top_to_bottom_until
  
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
    type t = Value.node value_distance [@@deriving sexp]

    (*  order reversed to get a max heap  *)
    let compare a b = Float.compare b.distance_to_target a.distance_to_target
    let of_node target g node = {
      distance_to_target=Distance.distance target (Value.value g node);
      node=node
    }
    let is_further a b = compare a b < 0
  end

  include PairingHeap(Element)

  let fold_far_to_near_until = fold_top_to_bottom_until
  let max = top
  let remove_max = remove_top
  let pop_max = pop_top
  let of_fold target g f =
    f ~init:(create ()) ~f:(fun acc node -> add acc (Element.of_node target g node))
end

module type NEAREST = sig
  (* maintain a set of n smallest values (typically implemented with a
     priority queue / max heap)

     Actually we want to maintain two sets of values:
     - ef (typically 5)
     - k (typically 100)
  *)
  type t_value_computer
  type node
  type value
  type t [@@deriving sexp]
  (* val create : t_value_computer -> value -> int -> t *)
  val create : t_value_computer -> value -> ef:int -> t
  type insert = Too_far | Inserted of t
  (* val insert : t -> node -> insert *)
  val insert_distance : t -> node value_distance -> insert
  (* val of_fold : (init:'acc -> f:('acc -> node -> 'acc) -> 'acc) -> t *)

  (*  in no particular order  *)
  val fold_distance : t -> init:'acc -> f:('acc -> node value_distance -> 'acc) -> 'acc
  val max_distance : t -> float
  val nearest_k : t -> int -> node value_distance list
end

module type VISIT_ME = sig
  type t_value_computer
  type nearest
  type node
  type value
  type t [@@deriving sexp]
  val singleton : t_value_computer -> value -> node value_distance -> t
  (* val add : t -> node -> t *)
  val add_distance : t -> node value_distance -> t
  val pop_nearest : t -> (node value_distance * t) option
  val nearest : t -> node option
  val of_nearest : nearest -> t
  (*  iterates in unspecified order  *)
  val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
end

module type SEARCH_GRAPH = sig
  type t [@@deriving sexp]
  type node [@@deriving sexp]

  module Visited : sig
    type t_graph = t
    type t [@@deriving sexp]
    val create : t_graph -> t
    (* type visit = Already_visited | New_visit of t *)
    (* val visit : t -> node -> visit *)
    val add : t -> node -> t
    val mem : t -> node -> bool
    val length : t -> int
  end

  module Neighbours : sig
    type t
    (* type nonrec node = node *)
    val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
  end

  val adjacent : t -> node -> Neighbours.t
  val num_nodes : t -> int
end

module Search
    (Graph : SEARCH_GRAPH)
    (VisitMe : VISIT_ME with type node = Graph.node)
    (Nearest : NEAREST with type node = Graph.node
                        and type value = VisitMe.value
                        and type t_value_computer = VisitMe.t_value_computer)
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
     not.

     Notes:

     - there are two numbers of nodes: k (the number of nearest
     neighbours we are searching for), and size_nearest = ef_, the
     number of candidate nodes we want to consider at a given time
     during search. We can have ef_ < k (actually, this is the typical
     case).

     - in nmslib, there are (at least) two strategies for combining
       the two;

        + have a separate fixed-size (k) priority queue for pushing
          the k nearest neighbours as we are pushing them into nearest

        + have a combined queue or sorted array where we can do both
          at the same time: keep the k best, AND keep the ef_ < k best

     nmslib seems to prefer the second option (the first is called
     "old")

     The current implementation below works only when ef_ =
     size_nearest >= k, which limits the potential of the algorithm.

     One possibility for accomodating both options would be to have
     Nearest be constructed with both ef_ and k, and manage things on
     its side as it sees fit (using two priority queues or one sorted
     array).

  *)
  (* let search *)
  (*     (hgraph : Value.t) *)
  (*     (graph : Graph.t) *)
  (*     (start_nodes : VisitMe.t) *)
  (*     (target : Value.value) *)
  (*     (size_nearest : int) = *)
  (*   let rec aux visit_me visited nearest = *)
  (*     (\*  notes: *)
  (*         - nodes in visit_me are possibly not already in visited *)
  (*         - in the usage here, Nearest should be a fixed-size *)
  (*         max-heap: inserting in the heap when it has reached max size *)
  (*         should either do nothing (the inserted is worse than the *)
  (*         existing max), or evict the previous max. *)
  (*     *\) *)
  (*     (\* printf "search aux:\n"; *\) *)
  (*     (\* printf "  visit-me:\n%s\n" (Sexp.to_string_hum @@ VisitMe.sexp_of_t visit_me); *\) *)
  (*     (\* printf "  visited:\n%s\n" (Sexp.to_string_hum @@ Graph.Visited.sexp_of_t visited); *\) *)
  (*     (\* printf "  nearest:\n%s\n" (Sexp.to_string_hum @@ Nearest.sexp_of_t nearest); *\) *)
  (*     match VisitMe.pop_nearest visit_me with *)
  (*     | None -> nearest *)
  (*     | Some (visit_node, visit_me) -> *)
  (*       match Graph.Visited.visit visited visit_node with *)
  (*       | Already_visited -> aux visit_me visited nearest *)
  (*       | New_visit visited -> *)
  (*         match Nearest.insert nearest visit_node with *)
  (*         | Too_far -> aux visit_me visited nearest *)
  (*         | InsertedNotEf nearest -> aux visit_me visited nearest *)
  (*         | InsertedEf nearest -> *)
  (*           let visit_me = *)
  (*             Graph.Neighbours.fold (Graph.adjacent graph visit_node) ~init:visit_me *)
  (*               ~f:(fun visit_me node -> *)
  (*                   if Graph.Visited.mem visited node then visit_me *)
  (*                   else VisitMe.add visit_me node) *)
  (*           in *)
  (*           aux visit_me visited nearest *)
  (*   in *)
  (*   (\* let visit_me = Graph.VisitMe.singleton start_node in *\) *)
  (*   let visited = Graph.Visited.create () in *)
  (*   let nearest = Nearest.create hgraph target size_nearest in *)
  (*   aux start_nodes visited nearest *)

  (* let search_one hgraph graph start_nodes target size_nearest = *)
  (*   (\* XXX this can be optimized: we do not need a priority queue for *)
  (*      nearest *\) *)
  (*   MinHeap.nearest target hgraph (Nearest.fold_ef (search hgraph graph start_nodes target size_nearest)) *)

  (*

 SEARCH-LEVEL(hnsw, q, epSet, ef, l)
1  visSet = epSet
2  cQueue = MIN-PRIORITY-QUEUE(epSet) // key – distance to q
3  wQueue = MAX-PRIORITY-QUEUE (epSet) // key – distance to q
4  while cQueue.size > 0
5    c = EXTRACT-MIN(cQueue)
6    if DIST(c, q) > DIST(MAX(wQueue), q)
7      break // all elements in the list are evaluated
8    for each e ∈ hnsw.adj(c, l) // update cQueue and wQueue
9      if e ∉ visSet
10       visSet = visSet ∪ e
11       if DIST(e, q) < DIST(MAX(wQueue), q)) or wQueue.size < ef
12         INSERT(cQueue, e)
13         INSERT(wQueue, e)
14         if wQueue.size > ef
15           EXTRACT-MAX(wQueue)
16 resQueue = MIN-PRIORITY-QUEUE(wQueue) // key – distance to q
17 return resQueue

cQueue = visit_me
wQueue = nearest

*)

  module Visited = Graph.Visited
  module Neighbours = Graph.Neighbours
  let distance = Distance.distance

  let visited_of_visit_me graph visit_me =
    VisitMe.fold visit_me ~init:(Visited.create graph) ~f:Visited.add

  let visited_singleton graph node =
    Visited.add (Visited.create graph) node

  let nearest_of_visit_me hgraph target visit_me ~size_nearest =
    VisitMe.fold visit_me ~init:(Nearest.create hgraph target ~ef:size_nearest)
      ~f:(fun (nearest : Nearest.t) node ->
          let element = { node; distance_to_target=distance (Value.value hgraph node) target } in
          match Nearest.insert_distance nearest element with
          | Too_far -> nearest
          | Inserted nearest -> nearest)

  let search (hgraph : Value.t) (graph : Graph.t) (start_nodes : VisitMe.t)
      (target : Value.value) (size_nearest : int) =

    let visit ((visit_me, nearest, visited) as acc) neighbour =
      if Visited.mem visited neighbour then acc
      else begin
        let visited = Visited.add visited neighbour in
        let neighbour_distance = {
          node = neighbour; distance_to_target = distance (Value.value hgraph neighbour) target
        } in
        match Nearest.insert_distance nearest neighbour_distance with
        | Too_far -> (visit_me, nearest, visited)
        | Inserted nearest ->
          let visit_me = VisitMe.add_distance visit_me neighbour_distance in
          (visit_me, nearest, visited)
      end
    in
    let rec aux visit_me nearest visited =
      (* printf "search: visited %d/%d\n%!" (Visited.length visited) (Graph.num_nodes graph);
       * printf "  visit_me: %s\n" (tsh [%sexp_of : VisitMe.t] visit_me);
       * printf "  nearest: %s\n" (tsh [%sexp_of : Nearest.t] nearest); *)

      (*  XXX TODO check this, it seems we are always visiting almost all the graph  *)
      match VisitMe.pop_nearest visit_me with
      | None -> nearest
      | Some (c, visit_me) ->
        if Float.(c.distance_to_target > Nearest.max_distance nearest) then nearest
        else begin
          let visit_me, nearest, visited = Neighbours.fold (Graph.adjacent graph c.node)
              ~init:(visit_me, nearest, visited)
              ~f:visit
          in aux visit_me nearest visited
        end
    in
    aux start_nodes (nearest_of_visit_me hgraph target start_nodes ~size_nearest)
      (visited_of_visit_me graph start_nodes)

  let search_one hgraph graph (start_node : Graph.node value_distance) target =

    let visit ((visit_me, (nearest : Graph.node value_distance), visited) as acc) neighbour =
      if Visited.mem visited neighbour then acc
      else begin
        let visited = Visited.add visited neighbour in
        let neighbour_distance = {
          node = neighbour; distance_to_target = distance (Value.value hgraph neighbour) target
        } in
        if Float.(neighbour_distance.distance_to_target >= nearest.distance_to_target) then
          (visit_me, nearest, visited)
        else
          let visit_me = VisitMe.add_distance visit_me neighbour_distance in
          (visit_me, neighbour_distance, visited)
      end
    in
    let rec aux visit_me (nearest : Graph.node value_distance) visited =
      (* printf "search_one: visited %d/%d\n%!" (Visited.length visited) (Graph.num_nodes graph);
       * printf "  visit_me: %s\n" (tsh [%sexp_of : VisitMe.t] visit_me);
       * printf "  nearest: %s\n" (tsh [%sexp_of : Graph.node value_distance] nearest); *)
      match VisitMe.pop_nearest visit_me with
      | None -> nearest
      | Some (c, visit_me) ->
        (* printf "  picked: %s\n" (tsh [%sexp_of : Graph.node value_distance] c); *)
        if Float.(c.distance_to_target > nearest.distance_to_target) then nearest
        else begin
          let visit_me, nearest, visited = Neighbours.fold (Graph.adjacent graph c.node)
              ~init:(visit_me, nearest, visited)
              ~f:visit
          in aux visit_me nearest visited
        end
    in
    (* let start_element = { *)
    (*   node=start_node; distance_to_target=distance (Value.value hgraph start_node) target *)
    (* } *)
    (* in *)
    aux (VisitMe.singleton hgraph target start_node) start_node (visited_singleton graph start_node.node)
end

module type HGRAPH_BASE = sig
  type t [@@deriving sexp]
  type node [@@deriving sexp]
  type value [@@deriving sexp]
 
  module LayerGraph : sig
    (* This must satisfy SEARCH_GRAPH and NEIGHBOUR_GRAPH.  *)
    type t [@@deriving sexp]
    (* type nonrec node = node [@@deriving sexp] *)
    (* type nonrec value = value [@@deriving sexp] *)

    module Visited : sig
      type t_graph = t
      type t [@@deriving sexp]
      val create : t_graph -> t
      (* type visit = Already_visited | New_visit of t *)
      (* val visit : t -> node -> visit *)
      val mem : t -> node -> bool
      val add : t -> node -> t
      val length : t -> int
    end

    module Neighbours : sig
      type t
      (* type nonrec node = node *)
      val create : unit -> t
      val add : t -> node -> t
      val length : t -> int
      val for_all : t -> f:(node -> bool) -> bool
      val fold : t -> init:'acc -> f:('acc -> node -> 'acc) -> 'acc
      val is_empty : t -> bool
    end

    val adjacent : t -> node -> Neighbours.t
    val num_nodes : t -> int
  end

  val is_empty : t -> bool

  val value : t -> node -> value
  (* val distance : value -> value -> float *)

  val layer : t -> int -> LayerGraph.t

  val max_layer : t -> int
  val set_max_layer : t -> int -> t

  val entry_point : t -> node
  val set_entry_point : t -> node -> t

  (*  overwrite connections of a node  *)
  val set_connections : t -> int -> node -> LayerGraph.Neighbours.t -> t
end

module type HGRAPH_INCR = sig
  include HGRAPH_BASE

  val create : unit -> t
  
  (*  add a new node with a given value, and given neighbours  *)
  (* val insert_in_layer : t -> int -> value -> LayerGraph.Neighbours.t -> t *)
  val allocate : t -> value -> (t * node)
end

module type HGRAPH = sig
  include HGRAPH_BASE
  module Values : sig
    type t [@@deriving sexp]
    (* type value [@@deriving sexp] *)
    val foldi : t -> init:'acc -> f:('acc -> int -> value -> 'acc) -> 'acc
  end
  
  val create : Values.t -> t
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
    val is_empty : t -> bool
  end

  val adjacent : t -> node -> Neighbours.t
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
      (current_neighbour_fold : init:'acc -> f:('acc -> Graph.node value_distance -> 'acc) -> 'acc)
      (num_neighbours : int) : Graph.Neighbours.t =
    let is_closer (node : MinHeap.Element.t) neighbours =
      (* XXX added this if, trying not to cut nodes out completely
         (not sufficient! would need to modify the fold logic) -- it
         seems to work in practice -- XXX TODO this could be done only
         when modifying an existing node (pass a boolean to indicate),
         and not when creating a new node *)
      if Graph.Neighbours.length (Graph.adjacent g node.node) <= 1 then true
      else let node_value = value node.node in
      Graph.Neighbours.for_all neighbours ~f:(fun neighbour ->
          Float.(>) (distance node_value (value neighbour)) node.distance_to_target
        )
    in
    let min_heap = MinHeap.of_fold_distance target hgraph current_neighbour_fold in
    let ret = MinHeap.fold_near_to_far_until min_heap ~init:(Graph.Neighbours.create ()) ~f:(fun neighbours node ->
        if is_closer node neighbours then begin
          let neighbours = Graph.Neighbours.add neighbours node.node in
          if Graph.Neighbours.length neighbours >= num_neighbours then MinHeap.Stop neighbours
          else MinHeap.Continue neighbours
        end else MinHeap.Continue neighbours
      )
    in
    assert (not @@ Graph.Neighbours.is_empty ret);
    ret
end

module BuildBase
    (Hgraph : HGRAPH_BASE)
    (VisitMe : VISIT_ME with type node = Hgraph.node
                         and type value = Hgraph.value
                         and type t_value_computer = Hgraph.t)
    (Nearest : NEAREST with type node = Hgraph.node
                        and type value = Hgraph.value
                        and type t_value_computer = Hgraph.t
                        and type t = VisitMe.nearest)
    (Distance : DISTANCE with type value = Hgraph.value) = struct
  module Layer = struct
    type node = Hgraph.node [@@deriving sexp]
    include Hgraph.LayerGraph
  end
  module SearchLayer = Search(Layer)(VisitMe)(Nearest)(Distance)(Hgraph)
  module SelectNeighbours = SelectNeighbours(Layer)(Distance)(Hgraph)

  let insert_knowing_node hgraph point_node point ~num_neighbours ~max_num_neighbours ~max_num_neighbours0
      ~num_neighbours_search ~level_mult =

    let value_distance node =
      { node; distance_to_target=Distance.distance (Hgraph.value hgraph node) point }
    in

    let insert_in_layer_no_neighbours hgraph i_layer value =
      (* let hgraph, node = Hgraph.allocate hgraph value in *)
      let hgraph = Hgraph.set_connections hgraph i_layer point_node (Layer.Neighbours.create ()) in
      Hgraph.set_entry_point hgraph point_node
    in

    if Hgraph.is_empty hgraph then insert_in_layer_no_neighbours hgraph 0 point else
      (* let hgraph, point_node = Hgraph.allocate hgraph point in *)
      let level = Int.of_float @@ Float.round_nearest @@ -. (Float.log (Random.float 1.)) *. level_mult in
      (* printf "inserting on level %d\n%!" level; *)

      let rec search_upper_layers i_layer (start_node : Hgraph.node value_distance) =
        if i_layer <= level then start_node, i_layer
        else
          let start_node =
            (* printf "insert: search_one in upper layer %d\n%!" i_layer; *)
            SearchLayer.search_one hgraph (Hgraph.layer hgraph i_layer) start_node point
          in
          search_upper_layers (i_layer - 1) start_node
      in

      let rec insert_in_lower_layers hgraph i_layer start_nodes =
        if i_layer < 0 then hgraph, start_nodes else begin
          let layer_graph = Hgraph.layer hgraph i_layer in
          let max_num_neighbours = if i_layer = 0 then max_num_neighbours0 else max_num_neighbours in
          (* printf "insert: search in lower layer %d\n%!" i_layer; *)
          let nearest = SearchLayer.search hgraph layer_graph start_nodes point num_neighbours_search in
          let neighbours = SelectNeighbours.select_neighbours
              hgraph
              ~value:(Hgraph.value hgraph)
              ~distance:Distance.distance
              layer_graph point (Nearest.fold_distance nearest) num_neighbours
          in
          let hgraph = Hgraph.set_connections hgraph i_layer point_node neighbours in
          let hgraph = Layer.Neighbours.fold neighbours ~init:hgraph
              ~f:(fun (hgraph : Hgraph.t) (node : Layer.node) ->
                  (* recompute this since we changed hgraph above  *)
                  let layer_graph = Hgraph.layer hgraph i_layer in
                  let connections = Layer.adjacent layer_graph node in
                  (* XXX would it be useful/fast to compute the
                     distances of all connections to the target in a
                     batch?  *)
                  if Layer.Neighbours.length connections > max_num_neighbours then begin
                    let new_connections =
                      SelectNeighbours.select_neighbours hgraph layer_graph
                        ~value:(Hgraph.value hgraph)
                        ~distance:Distance.distance
                        (Hgraph.value hgraph node)
                        (fun ~init ~f ->
                           Layer.Neighbours.fold connections ~init
                             ~f:(fun acc node -> f acc (value_distance node)))
                        max_num_neighbours
                    in
                    Hgraph.set_connections hgraph i_layer node new_connections
                  end else hgraph
                )
          in insert_in_lower_layers hgraph (i_layer - 1) (VisitMe.of_nearest nearest)
        end
      in

      let search hgraph =
        let closest_node, layer_level =
          search_upper_layers (Hgraph.max_layer hgraph) (value_distance (Hgraph.entry_point hgraph))
        in
        insert_in_lower_layers hgraph layer_level (VisitMe.singleton hgraph point closest_node)
      in

      let hgraph, nearest = search hgraph in
      (* We use max_layer/set_max_layer to notice that we have to update
         the entry point. This is not satisfying because the graph could
         manage its max layer by itself, since it knows about it. *)
      if level > (Hgraph.max_layer hgraph) then
        (* let point_node = match VisitMe.nearest nearest with *)
        (*   | Some node -> node *)
        (*   | None -> failwith "unexpected: visit-me set is empty!" *)
        (* in *)
        Hgraph.set_max_layer (Hgraph.set_entry_point hgraph point_node) level
      else hgraph
end

module BuildIncr
    (Hgraph : HGRAPH_INCR)
    (VisitMe : VISIT_ME with type node = Hgraph.node
                         and type value = Hgraph.value
                         and type t_value_computer = Hgraph.t)
    (Nearest : NEAREST with type node = Hgraph.node
                        and type value = Hgraph.value
                        and type t_value_computer = Hgraph.t
                        and type t = VisitMe.nearest)
    (Distance : DISTANCE with type value = Hgraph.value) = struct
  include BuildBase(Hgraph)(VisitMe)(Nearest)(Distance)
  
  (* module Layer = struct *)
  (*   type node = Hgraph.node [@@deriving sexp] *)
  (*   include Hgraph.LayerGraph *)
  (* end *)
  (* module SearchLayer = Search(Layer)(VisitMe)(Nearest)(Distance)(Hgraph) *)
  (* module SelectNeighbours = SelectNeighbours(Layer)(Distance)(Hgraph) *)

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

Notes:

- so what happens if max_layer = 1 and level = 42 ? max_layer gets set
  to 42, and the start node is the newly inserted node

 *)

  (* let rec aux (hgraph : Hgraph.t) (i_layer : int) (start_nodes : VisitMe.t) = *)
  (*   if i_layer < 0 then hgraph, start_nodes else begin *)
  (*     let layer_graph = Hgraph.layer hgraph i_layer in *)
  (*     let max_num_neighbours = if i_layer = 0 then max_num_neighbours0 else max_num_neighbours in *)
  (*     if i_layer > level then begin *)
  (*       (\*  layers above level: just search  *\) *)
  (*       match SearchLayer.search_one hgraph layer_graph start_nodes point num_neighbours_search_above with *)
  (*       | None -> failwith "unexpected: found no neighbour at all" *)
  (*       | Some nearest -> aux hgraph (i_layer - 1) *)
  (*                           (VisitMe.singleton hgraph point nearest.node) *)
  (*     end else begin *)
  (*       (\* layers equal or below level: search and insert the *)
  (*          node *\) *)
  (*       let nearest = SearchLayer.search hgraph layer_graph start_nodes point num_neighbours_search in *)
  (*       let neighbours = SelectNeighbours.select_neighbours *)
  (*           hgraph *)
  (*           ~value:(Hgraph.value hgraph) *)
  (*           ~distance:Distance.distance *)
  (*           layer_graph point (Nearest.fold nearest) num_neighbours *)
  (*       in *)
  (*       (\* let hgraph = Hgraph.insert hgraph i_layer point neighbours in *\) *)
  (*       let hgraph = Hgraph.set_connections hgraph i_layer point_node neighbours in *)
  (*       let hgraph = Layer.Neighbours.fold neighbours ~init:hgraph *)
  (*           ~f:(fun (hgraph : Hgraph.t) (node : Layer.node) -> *)
  (*               (\* recompute this since we changed hgraph above  *\) *)
  (*               let layer_graph = Hgraph.layer hgraph i_layer in *)
  (*               let connections = Layer.adjacent layer_graph node in *)
  (*               if Layer.Neighbours.length connections > max_num_neighbours then begin *)
  (*                 let new_connections = *)
  (*                   SelectNeighbours.select_neighbours hgraph layer_graph *)
  (*                     ~value:(Hgraph.value hgraph) *)
  (*                     ~distance:Distance.distance *)
  (*                     (Hgraph.value hgraph node) *)
  (*                     (Layer.Neighbours.fold connections) *)
  (*                     max_num_neighbours *)
  (*                 in *)
  (*                 Hgraph.set_connections hgraph i_layer node new_connections *)
  (*               end else hgraph *)
  (*             ) *)
  (*       in aux hgraph (i_layer - 1) (VisitMe.of_nearest nearest) *)
  (*     end *)
  (*   end *)
  (* in *)


  (* let insert_knowing_node hgraph point_node point ~num_neighbours ~max_num_neighbours ~max_num_neighbours0 *)
  (*     ~num_neighbours_search ~level_mult = *)

  (*   let value_distance node = *)
  (*     { node; distance_to_target=Distance.distance (Hgraph.value hgraph node) point } *)
  (*   in *)

  (*   let insert_in_layer_no_neighbours hgraph i_layer value = *)
  (*     (\* let hgraph, node = Hgraph.allocate hgraph value in *\) *)
  (*     let hgraph = Hgraph.set_connections hgraph i_layer point_node (Layer.Neighbours.create ()) in *)
  (*     Hgraph.set_entry_point hgraph point_node *)
  (*   in *)

  (*   if Hgraph.is_empty hgraph then insert_in_layer_no_neighbours hgraph 0 point else *)
  (*     (\* let hgraph, point_node = Hgraph.allocate hgraph point in *\) *)
  (*     let level = Int.of_float @@ Float.round_nearest @@ -. (Float.log (Random.float 1.)) *. level_mult in *)
  (*     (\* printf "inserting on level %d\n%!" level; *\) *)

  (*     let rec search_upper_layers i_layer (start_node : Hgraph.node value_distance) = *)
  (*       if i_layer <= level then start_node, i_layer *)
  (*       else *)
  (*         let start_node = *)
  (*           SearchLayer.search_one hgraph (Hgraph.layer hgraph i_layer) start_node point *)
  (*         in search_upper_layers (i_layer - 1) start_node *)
  (*     in *)

  (*     let rec insert_in_lower_layers hgraph i_layer start_nodes = *)
  (*       if i_layer < 0 then hgraph, start_nodes else begin *)
  (*         let layer_graph = Hgraph.layer hgraph i_layer in *)
  (*         let max_num_neighbours = if i_layer = 0 then max_num_neighbours0 else max_num_neighbours in *)
  (*         let nearest = SearchLayer.search hgraph layer_graph start_nodes point num_neighbours_search in *)
  (*         let neighbours = SelectNeighbours.select_neighbours *)
  (*             hgraph *)
  (*             ~value:(Hgraph.value hgraph) *)
  (*             ~distance:Distance.distance *)
  (*             (\* XXX Nearest.fold throws out the distance, this is a *)
  (*                pity, it is reconstructed by the MinHeap created by *)
  (*                SelectNeighbours *\) *)
  (*             layer_graph point (Nearest.fold nearest) num_neighbours *)
  (*         in *)
  (*         let hgraph = Hgraph.set_connections hgraph i_layer point_node neighbours in *)
  (*         let hgraph = Layer.Neighbours.fold neighbours ~init:hgraph *)
  (*             ~f:(fun (hgraph : Hgraph.t) (node : Layer.node) -> *)
  (*                 (\* recompute this since we changed hgraph above  *\) *)
  (*                 let layer_graph = Hgraph.layer hgraph i_layer in *)
  (*                 let connections = Layer.adjacent layer_graph node in *)
  (*                 (\* XXX would it be useful/fast to compute the *)
  (*                    distances of all connections to the target in a *)
  (*                    batch?  *\) *)
  (*                 if Layer.Neighbours.length connections > max_num_neighbours then begin *)
  (*                   let new_connections = *)
  (*                     SelectNeighbours.select_neighbours hgraph layer_graph *)
  (*                       ~value:(Hgraph.value hgraph) *)
  (*                       ~distance:Distance.distance *)
  (*                       (Hgraph.value hgraph node) *)
  (*                       (Layer.Neighbours.fold connections) *)
  (*                       max_num_neighbours *)
  (*                   in *)
  (*                   Hgraph.set_connections hgraph i_layer node new_connections *)
  (*                 end else hgraph *)
  (*               ) *)
  (*         in insert_in_lower_layers hgraph (i_layer - 1) (VisitMe.of_nearest nearest) *)
  (*       end *)
  (*     in *)

  (*     let search hgraph = *)
  (*       let closest_node, layer_level = *)
  (*         search_upper_layers (Hgraph.max_layer hgraph) (value_distance (Hgraph.entry_point hgraph)) *)
  (*       in *)
  (*       insert_in_lower_layers hgraph layer_level (VisitMe.singleton hgraph point closest_node) *)
  (*     in *)

  (*     let hgraph, nearest = search hgraph in *)
  (*     (\* We use max_layer/set_max_layer to notice that we have to update *)
  (*        the entry point. This is not satisfying because the graph could *)
  (*        manage its max layer by itself, since it knows about it. *\) *)
  (*     if level > (Hgraph.max_layer hgraph) then *)
  (*       (\* let point_node = match VisitMe.nearest nearest with *\) *)
  (*       (\*   | Some node -> node *\) *)
  (*       (\*   | None -> failwith "unexpected: visit-me set is empty!" *\) *)
  (*       (\* in *\) *)
  (*       Hgraph.set_max_layer (Hgraph.set_entry_point hgraph point_node) level *)
  (*     else hgraph *)

  let insert hgraph point =
    let hgraph, node = Hgraph.allocate hgraph point in
    insert_knowing_node hgraph node point
  
  let create point_fold ~num_neighbours ~max_num_neighbours ~max_num_neighbours0
      ~num_neighbours_search ~level_mult =
    point_fold ~init:(Hgraph.create ()) ~f:(fun hgraph point ->
        let hgraph =
          insert hgraph point ~num_neighbours ~max_num_neighbours ~max_num_neighbours0
            ~num_neighbours_search ~level_mult
        in
        (* printf "graph after insert of %s:\n%s\n%!" *)
        (*   (Sexp.to_string_hum @@ [%sexp_of : Hgraph.value] point) *)
        (*   (Sexp.to_string_hum @@ [%sexp_of : Hgraph.t] hgraph); *)
        hgraph)
end

module BuildBatch
      (Hgraph : HGRAPH with type node = int)
      (VisitMe : VISIT_ME with type node = Hgraph.node
                           and type value = Hgraph.value
                           and type t_value_computer = Hgraph.t)
      (Nearest : NEAREST with type node = Hgraph.node
                          and type value = Hgraph.value
                          and type t_value_computer = Hgraph.t
                          and type t = VisitMe.nearest)
      (Distance : DISTANCE with type value = Hgraph.value) = struct
    include BuildBase(Hgraph)(VisitMe)(Nearest)(Distance)

  let create points ~num_neighbours ~max_num_neighbours ~max_num_neighbours0
      ~num_neighbours_search ~level_mult =
    Hgraph.Values.foldi points ~init:(Hgraph.create points) ~f:(fun hgraph i point ->
        insert_knowing_node hgraph i point ~num_neighbours ~max_num_neighbours ~max_num_neighbours0
          ~num_neighbours_search ~level_mult)
end

module type KNN_HGRAPH = sig
  type t
  type value [@@deriving sexp]
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

  let knn hgraph target ?(num_neighbours_search=5) ~num_neighbours =
    (* printf "knn: n_search: %d n:%d\n" num_neighbours_search num_neighbours; *)
    let rec search_upper i_layer start_node =
      if i_layer <= 0 then start_node
      else
        search_upper (i_layer-1)
          (Search.search_one hgraph (Hgraph.layer hgraph i_layer) start_node target)
    in

    let value_distance node =
      { node; distance_to_target=Distance.distance (Hgraph.value hgraph node) target }
    in
    
    let start_node_zero =
      search_upper (Hgraph.max_layer hgraph) (value_distance (Hgraph.entry_point hgraph))
    in
    let nearest =
      Search.search hgraph (Hgraph.layer hgraph 0) (VisitMe.singleton hgraph target start_node_zero)
        target num_neighbours_search
    in Nearest.nearest_k nearest num_neighbours

  (* let rec aux start_nodes i_layer = *)
  (*   (\* printf "knn: layer: %d start nodes:\n%s\n" i_layer *)
  (*      (Sexp.to_string_hum @@ VisitMe.sexp_of_t start_nodes); *\) *)
  (*   let layer = Hgraph.layer hgraph i_layer in *)
  (*   if i_layer <= 0 then *)
  (*     let neighbours = *)
  (*       Search.search hgraph layer start_nodes target num_neighbours_search num_neighbours *)
  (*     in *)
  (*     (\* printf "knn: final search for neighbours:\n%s\n" *)
  (*        (Sexp.to_string_hum @@ Nearest.sexp_of_t neighbours); *\) *)
  (*     MinHeap.nearest_k num_neighbours target hgraph (Nearest.fold neighbours) *)
  (*   else *)
  (*     match Search.search_one hgraph layer start_nodes target 1 with *)
  (*     | None -> failwith "unexpected: found no neighbour at all" *)
  (*     | Some nearest -> *)
  (*       aux *)
  (*         (VisitMe.singleton hgraph target nearest.node) *)
  (*         (i_layer - 1) *)
  (* in *)
    (* aux *)
    (*   (VisitMe.singleton hgraph target (Hgraph.entry_point hgraph)) *)
    (*   (Hgraph.max_layer hgraph) *)
end

