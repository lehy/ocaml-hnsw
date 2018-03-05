open Base
open Stdio

let random_data dim n =
  List.init n ~f:(fun _ -> Array.init dim ~f:(fun _ -> Random.float 100.))

(* module type DISTANCE = sig *)
(*   type t [@@deriving sexp] *)
(*   val distance : t -> t -> float *)
(* end *)

module type DISTANCE = sig
  type value [@@deriving sexp]
  val distance : value -> value -> float
end

module Hgraph(Distance : DISTANCE) = struct
  type node = int [@@deriving sexp]
  type value = Distance.value [@@deriving sexp]

  module LayerGraph = struct
    type nonrec node = node [@@deriving sexp]
    type nonrec value = value [@@deriving sexp]

    module Visited = struct
      type t = Set.M(Int).t [@@deriving sexp]
      let create () = Set.empty (module Int)
      type visit = Already_visited | New_visit of t
      let visit visited node =
        let new_visited = Set.add visited node in
        if phys_equal new_visited visited then Already_visited
        else New_visit new_visited
      let mem visited node = Set.mem visited node
    end

    module Neighbours = struct
      (* gah! I wish I knew how to just say module Neighbours = Set
         with type t = Set.M(Int) or whatever *)
      type t = Set.M(Int).t [@@deriving sexp]
      type nonrec node = node

      let create () = Set.empty (module Int)
      let add n node = Set.add n node
      let length c = Set.length c
      let for_all n ~f = Set.for_all n ~f
      let fold c ~init ~f =
        Set.fold c ~init ~f

      let diff a b =
        Set.diff a b

      let union a b = Set.union a b
      let for_all x ~f = Set.for_all x ~f
    end

    type t = {
      (* values : value Map.M(Int).t; *)
      connections : Neighbours.t Map.M(Int).t;
      (* next_available_node : int *)
    } [@@deriving sexp]

    (*  XXX TODO: check symmetric  *)
    let invariant x = true
    (* let invariant x = *)
    (*   Map.for_alli x.connections ~f:(fun ~key ~data -> *)
    (*       Map.mem x.values key && Neighbours.for_all data ~f:(Map.mem x.values)) *)

    (*  XXX code duplication!  *)
    (* exception Value_not_found of string *)
    (* let value g node = *)
    (*   match Map.find g.values node with *)
    (*   | None -> raise (Value_not_found (Printf.sprintf "node: %s\nvalues:\n%s\nconnections:\n%s" *)
    (*                                       (Sexp.to_string_hum (sexp_of_node node)) *)
    (*                                       (Sexp.to_string_hum *)
    (*                                          ([%sexp_of : Map.M(Int).t] sexp_of_value g.values)) *)
    (*                                       (Sexp.to_string_hum *)
    (*                                          ([%sexp_of : Map.M(Int).t] Neighbours.sexp_of_t g.connections)))) *)
    (*   | Some node -> node *)
    (* Map.find_exn g.values node *)
    (* Map.find_exn g.values node *)

    let create values =
      { (* values; *)
        connections = Map.empty (module Int);
        (* next_available_node = 0 *) }

    let fold_neighbours g node ~init ~f =
      match Map.find g.connections node with
      | None -> init
      | Some neighbours ->
        Set.fold neighbours ~init ~f

    let distance a b = Distance.distance a b

    let adjacent g node =
      match Map.find g.connections node with
      | None -> Set.empty (module Int)
      | Some neighbours -> neighbours

    let connect_symmetric g node neighbours =
      assert (invariant g);
      (* assert (Neighbours.for_all neighbours ~f:(Map.mem g.values)); *)
      (* assert (Map.mem g.values node); *)
      let connections = Map.set g.connections node neighbours in
      let g =
        { connections = Neighbours.fold neighbours ~init:connections
              ~f:(fun connections neighbour ->
                  let neighbours_of_neighbour = match Map.find connections neighbour with
                    | None -> Set.singleton (module Int) node
                    | Some old_neighbours -> Set.add old_neighbours node
                  in
                  Map.set connections neighbour neighbours_of_neighbour) }
      in
      assert (invariant g);
      g

    (* let insert g value neighbours = *)
    (*   let connect_symmetric connections node neighbours = *)
    (*     let connections = Map.set connections node neighbours in *)
    (*     Neighbours.fold neighbours ~init:connections ~f:(fun connections neighbour -> *)
    (*         let neighbours_of_neighbour = match Map.find connections neighbour with *)
    (*           | None -> Set.singleton (module Int) node *)
    (*           | Some old_neighbours -> Set.add old_neighbours node *)
    (*         in *)
    (*         Map.set connections neighbour neighbours_of_neighbour) *)
    (*   in *)
    (*   TODO *)
    (*     solve the node registry problem: *)
    (*     maybe the layergraph abstraction does not work? how to insert a node in there? *)
    (*     inserting a node into the layergraph should modify the hgraph as well (since it holds values) *)
    (*   { (\* values = Map.set g.values g.next_available_node value; *\) *)
    (*     connections = connect_symmetric g.connections g.next_available_node neighbours; *)
    (*     (\* next_available_node = g.next_available_node + 1 *\) } *)

    let add_neighbours g node added_neighbours =
      (* let connections = Map.set g.connections node (Neighbours.union (adjacent g node) added_neighbours) in *)
      let connections = Map.update g.connections node (function
          | None -> added_neighbours
          | Some old_neighbours -> Neighbours.union old_neighbours added_neighbours) in
      let connections = Neighbours.fold added_neighbours ~init:connections
          ~f:(fun connections added_neighbour ->
              Map.update connections added_neighbour ~f:(function
                  | None -> Set.singleton (module Int) node
                  | Some old -> Set.add old node))
      in let g = { connections } in
      assert (invariant g);
      g

    let remove_neighbours g node removed_neighbours =
      let connections = Map.update g.connections node (function
          | None -> Neighbours.create ()
          | Some old -> Neighbours.diff old removed_neighbours) in
      let connections = Neighbours.fold removed_neighbours ~init:connections
          ~f:(fun connections removed_neighbour ->
              Map.update connections removed_neighbour ~f:(function
                  | None -> Set.empty (module Int)
                  | Some old -> Set.remove old node))
      in let g = { connections } in
      assert (invariant g);
      g
  end

  type t = {
    layers : LayerGraph.t Map.M(Int).t;
    max_layer : int;
    entry_point : node;
    values : value Map.M(Int).t;
    next_available_node : int
  } [@@deriving sexp]

  exception Value_not_found of string
  let value h node =
    match Map.find h.values node with
    | None -> raise (Value_not_found (Printf.sprintf "node: %s\nvalues:\n%s"
                                        (Sexp.to_string_hum (sexp_of_node node))
                                        (Sexp.to_string_hum ([%sexp_of : Map.M(Int).t] sexp_of_value h.values))))
    | Some node -> node

  (*  not great! entry_point is invalid when the net is empty!  *)
  let create () =
    { layers = Map.empty (module Int);
      max_layer = 0;
      entry_point = 0;
      values = Map.empty (module Int);
      next_available_node = 0 }

  let is_empty h = Map.is_empty h.values

  (* let layer hgraph i = Map.find_exn hgraph.layers i *)
  let layer hgraph i = match Map.find hgraph.layers i with
    | Some layer -> layer
    | None -> LayerGraph.create hgraph.values

  let max_layer hgraph = hgraph.max_layer
  let set_max_layer hgraph m = { hgraph with max_layer = m }

  (* Not well defined when the graph is empty, and no check that the
     entry point is an actual node. However the neighbour sets should
     be returned as empty if the node is not found, so maybe this is
     not a problem. *)
  let entry_point h = h.entry_point
  let set_entry_point h p = { h with entry_point = p }

  let allocate_node h =
    h.next_available_node, { h with next_available_node = h.next_available_node+1 }

  let allocate h value =
    let node, h = allocate_node h in
    { h with values = Map.set h.values node value }, node
  
  let invariant h =
    Map.for_all h.layers ~f:LayerGraph.invariant

  let insert h i_layer value neighbours =
    let h, node = allocate h value in
    assert (invariant h);
    assert (Map.mem h.values node);
    let layer = layer h i_layer in
    assert (LayerGraph.invariant layer);
    let updated_layer = LayerGraph.connect_symmetric layer node neighbours in
    let h = { h with layers = Map.set h.layers i_layer updated_layer } in
    assert (invariant h);
    h

  let set_connections h i_layer node neighbours =
    let old_neighbours = LayerGraph.adjacent (layer h i_layer) node in
    let removed_neighbours = LayerGraph.Neighbours.diff old_neighbours neighbours in
    let added_neighbours = LayerGraph.Neighbours.diff neighbours old_neighbours in
    let layer = layer h i_layer in
    let layer = LayerGraph.add_neighbours layer node added_neighbours in
    let layer = LayerGraph.remove_neighbours layer node removed_neighbours in
    { h with layers = Map.set h.layers i_layer layer }
end

(* module Value(Distance : DISTANCE) = struct *)
(*   module Hgraph = Hgraph(Distance) *)
(*   type t = Hgraph.t [@@deriving sexp] *)
(*   type node = Hgraph.node [@@deriving sexp] *)
(*   type value = Hgraph.value *)
(*   let value g n = Hgraph.value g n *)
(* end *)

module Nearest(Distance : DISTANCE) = struct
  module Hgraph = Hgraph(Distance)
  type t_value_computer = Hgraph.t [@@deriving sexp]
  type node = Hgraph.node [@@deriving sexp]
  type value = Hgraph.value [@@deriving sexp]
  module MaxHeap = Hnsw.MaxHeap(Distance)(Hgraph)

  (* type graph = t [@@deriving sexp] *)
  type t = { value_computer : t_value_computer sexp_opaque;
             target : value;
             size : int;
             max_size : int;
             max_priority_queue : MaxHeap.t } [@@deriving sexp]

  let value_computer x = x.value_computer
  let target x = x.target

  let create value_computer target max_size =
    { value_computer; target; size=0; max_size; max_priority_queue=MaxHeap.create () }

  let length q = q.size

  type insert = Too_far | Inserted of t
  let insert q node =
    let new_element = MaxHeap.Element.of_node q.target q.value_computer node in
    let size = length q in
    if size < q.max_size then
      Inserted { q with max_priority_queue = MaxHeap.add q.max_priority_queue new_element; size=q.size+1 }
    else if not @@ MaxHeap.Element.is_further new_element (Option.value_exn (MaxHeap.max q.max_priority_queue)) then
      let new_heap = MaxHeap.add q.max_priority_queue new_element |> MaxHeap.remove_max in
      Inserted { q with max_priority_queue = new_heap }
    else Too_far
  let fold x ~init ~f = MaxHeap.fold x.max_priority_queue ~init ~f:(fun acc e -> f acc e.node)
  let fold_distance x ~init ~f = MaxHeap.fold x.max_priority_queue ~init ~f:(fun acc e -> f acc e)
end

module VisitMe(Distance : DISTANCE) = struct
  module Hgraph = Hgraph(Distance)
  module Nearest = Nearest(Distance)
  module MinHeap = Hnsw.MinHeap(Distance)(Hgraph)
  type t_value_computer = Hgraph.t [@@deriving sexp]
  type value = Hgraph.value [@@deriving sexp]
  type t = { target : value;
             value_computer : t_value_computer sexp_opaque;
             heap : MinHeap.t } [@@deriving sexp]
  type node = Hgraph.node
  type nearest = Nearest.t
  
  let singleton value_computer target n =
    { target; value_computer;
      heap = MinHeap.singleton (MinHeap.Element.of_node target value_computer n) }

  let of_nearest nearest =
    { target = Nearest.target nearest;
      value_computer = Nearest.value_computer nearest;
      heap = Nearest.fold_distance nearest ~init:(MinHeap.create ()) ~f:(fun h e ->
          MinHeap.add h { node = e.node; distance_to_target = e.distance_to_target }) }
  let nearest (v : t) =
    match MinHeap.min v.heap with
    | None -> None
    | Some node -> Some node.MinHeap.Element.node
  let pop_nearest (v : t) =
    match MinHeap.pop_min v.heap with
    | None -> None
    | Some (n, h) -> Some (n.MinHeap.Element.node, { v with heap = h })
  let add (v : t) node =
    { v with heap = MinHeap.add v.heap (MinHeap.Element.of_node v.target v.value_computer node) }
end

module EuclideanDistance = struct
  type value = float array [@@deriving sexp]
  let distance (a : value) (b : value) =
    if Array.length a <> Array.length b then
      raise (Invalid_argument "distance: arrays with different lengths");
    let ret = ref 0. in
    for i = 0 to Array.length a - 1 do
      let diff = a.(i) -. b.(i) in
      ret := !ret +. diff *. diff
    done;
    Float.sqrt(!ret)
end
module HgraphEuclidean = Hgraph(EuclideanDistance)
module Build = Hnsw.Build
    (HgraphEuclidean)
    (VisitMe(EuclideanDistance))
    (Nearest(EuclideanDistance))
    (EuclideanDistance)
(*
    level_mult = 1 / log(M)

    M_max_0 = 2 * M
    they have M_max for layers > 0 and M_max_0 for layer 0
    (they say these are separate but say nothing about M_max, so I suppose I could let them be equal)
    M_max is Hgraph.max_num_neighbours

    M in 5..48 (higher: for higher dim, higher recall, drives memory consumption)
    M == num_neighbours passed to Build.insert and Build.create
    It is the number of neighbours one selects when inserting one
    given point. If adding these neighbours makes one node's
    neighbours be more than M_max, the neighbours are truncated.

    efConstruction can be autoconfigured (how ?), gives example of
    100, should allow high recall (0.95) during construction
    efConstruction == num_neighbours_search passed to Build.insert and Build.create

    ef used during search
    ef == num_neighbours + num_additional_neighbours_search in Search.search and Knn.knn
    Not clear how to configure. I suppose having it in 0..~M is reasonable.

    TODO: look at code and benchmarks, see how they configure ef and efConstruction and M.
 *)
let build_hgraph data =
  let num_neighbours = 3 in
  let max_num_neighbours = 2 * num_neighbours in
  let num_neighbours_search = 7 in
  let level_mult = 1. /. Float.log (Float.of_int num_neighbours) in
  Build.create (List.fold_left data)
    ~num_neighbours ~max_num_neighbours ~num_neighbours_search ~level_mult

module Knn = Hnsw.Knn
    (HgraphEuclidean)
    (VisitMe(EuclideanDistance))
    (Nearest(EuclideanDistance))
    (EuclideanDistance)

let knn hgraph point num_neighbours =
  let num_additional_neighbours_search = 1 in
  Knn.knn hgraph point ~num_neighbours ~num_additional_neighbours_search

let show_hgraph hgraph filename = ()
let show_neighbours hgraph point neighbours filename = ()

let test_graphical () =
  let dim = 2 in
  let n_train = 20 in
  let n_test = 5 in
  let train_data = random_data dim n_train in
  let test_data = random_data dim n_test in
  printf "train data:\n%s\n" (Sexp.to_string_hum @@ [%sexp_of : float array list] train_data);
  printf "test data:\n%s\n" (Sexp.to_string_hum @@ [%sexp_of : float array list] test_data);
  let hgraph = build_hgraph train_data in
  printf "graph:\n%s\n" (Sexp.to_string_hum @@ HgraphEuclidean.sexp_of_t hgraph);
  show_hgraph hgraph "hgraph.svg";
  let i = ref 0 in
  List.iter test_data ~f:(fun point ->
      let neighbours = knn hgraph point 3 in
      printf "neighbours of %s:\n%s\n"
        (Sexp.to_string_hum @@ [%sexp_of : float array] point)
        (Sexp.to_string_hum @@ [%sexp_of : Knn.MinHeap.Element.t list] neighbours);
      show_neighbours hgraph point neighbours (Printf.sprintf "neighbours_%03d.svg" !i);
      Int.incr i);;
(*  TODO:
- print to dot
- write simple (expect?) tests
- run on benchmark
*)

test_graphical ();;
