open Core_kernel


(** A minimal extensible array.  *)
module Vector = struct
  type 'a t = {
    mutable buf : 'a array;
    mutable size : int;
    empty_element : 'a
  } [@@deriving sexp]

  let capacity v = Array.length v.buf

  let check_index_ v i =
    if i < 0 || i >= v.size then invalid_arg "Vector.get"

  let extend_to_ v ~new_capacity =
    let new_buf = Array.create ~len:new_capacity v.empty_element in
    Array.blit v.buf 0 new_buf 0 (Array.length v.buf);
    v.buf <- new_buf

  let extend_ v =
    extend_to_ v ~new_capacity:(2 * (capacity v) + 10)

  let create empty_element = { buf = [||]; size = 0; empty_element }
  let create_with ?(capacity=128) empty_element =
    { buf = Array.create ~len:capacity empty_element; size = 0; empty_element }

  let init n ~f empty_element =
    {
      buf = Array.init n ~f;
      size = n;
      empty_element
    }

  let reserve v ~new_capacity =
    if new_capacity > (capacity v) then
      extend_to_ v ~new_capacity 

  let iter v ~f =
    for i = 0 to v.size - 1 do
      f v.buf.(i)
    done

  let for_all v ~f =
    let exception For_all_failure in
    try
      iter v ~f:(fun x -> if not @@ f x then raise For_all_failure);
      true
    with For_all_failure -> false

  let fold v ~init ~f =
    let acc = ref init in
    for i = 0 to v.size - 1 do
      acc := f !acc v.buf.(i)
    done;
    !acc         

  let length v = v.size
  let is_empty v = length v = 0

  let get v i =
    check_index_ v i;
    v.buf.(i)
  let set v i x =
    check_index_ v i;
    v.buf.(i) <- x
  let push_back v x =
    if v.size >= Array.length v.buf then begin
      extend_ v
    end;
    v.buf.(v.size) <- x;
    v.size <- v.size + 1

  module Test = struct
    let element_exists a i =
      try let _ = get a i in true with _ -> false

    let%test _ = length (create 0) = 0
    let%test _ = length (create_with ~capacity:56 0) = 0
    let%test _ = capacity (create_with ~capacity:56 0) = 56
    let%test _ = let a = create 0 in push_back a 42; length a = 1
    let%test _ = let a = create 0 in not @@ element_exists a 0
    let%test _ = let a = create 0 in push_back a 1; element_exists a 0 && not @@ element_exists a 1
    let%test _ =
      let a = create_with ~capacity:1 0 in
      push_back a 1; push_back a 2; push_back a 3;
      fold a ~init:0 ~f:(+) = 6
    let%test _ =
      let a = create_with ~capacity:1 0 in
      push_back a 1; push_back a 2; push_back a 3;
      get a 2 = 3 && get a 0 = 1
    let%test _ =
      let a = create_with ~capacity:2 0 in
      push_back a 1; push_back a 2; push_back a 3;
      set a 1 42;
      get a 2 = 3 && get a 0 = 1 && get a 1 = 42
  end
end

module Neighbours = struct
  type t = { mutable list : int list; mutable length : int } [@@deriving sexp]

  let create () = { list=[]; length=0 }
  let singleton n = { list=[n]; length=1 }
  let add n node =
    n.list <- node::n.list;
    n.length <- n.length + 1
  let remove n node =
    let ret = create () in
    List.iter n.list ~f:(fun e ->
        if e <> node then add ret e);
    n.list <- ret.list;
    n.length <- ret.length
  let length c = c.length
  let for_all n ~f = List.for_all n.list ~f
  let iter c ~f = List.iter c.list ~f

  let diff_both a b =
    let sa = Set.of_list (module Int) a.list in
    let sb = Set.of_list (module Int) b.list in
    let dba = Set.diff sb sa in
    let dab = Set.diff sa sb in
    dba, dab

  let is_empty n = n.length = 0

  module Test = struct
    let mem n a = List.mem n.list a ~equal:Int.equal
    let of_list list = { list; length=List.length list }
    let to_sorted_list n = List.sort n.list ~compare:Int.compare

    let%test _ = length (create ()) = 0
    let%test _ = let a = create () in add a 42; add a 53; length a = 2
    let%test _ = let a = create () in add a 42; add a 53; remove a 42; length a = 1
    let%test _ = let a = create () in add a 42; add a 53; remove a 47; length a = 2
    let%test _ = let a = create () in add a 42; add a 53; for_all a ~f:(fun x -> x = 42 || x = 53)
    let%test _ = let a = create () in add a 42; add a 53; not @@ for_all a ~f:(fun x -> x = 42)
    let%test _ =
      let a = create () in
      add a 42; add a 53;
      let b = create () in
      add b 42; add b 57;
      let dba, dab = diff_both a b in
      Set.equal dba (Set.of_list (module Int) [57]) && Set.equal dab (Set.of_list (module Int) [53])
  end
end

(** An imperative graph. Nodes are identified by consecutive integers
    starting from zero.

*)
module Graph = struct
  type t = Neighbours.t Vector.t [@@deriving sexp]
  let empty : unit -> t = fun () -> Vector.create (Neighbours.create ())
  let create n =
    Vector.init n ~f:(fun i -> Neighbours.create ()) (Neighbours.create ())

  let num_nodes (g : t) = Vector.length g

  let adjacent g node = Vector.get g node

  let add_node g =
    Vector.push_back g (Neighbours.create ())

  let iter_neighbours g ~f =
    let _ = Vector.fold g ~init:0 ~f:(fun i neighbours ->
        f i neighbours;
        i+1)
    in ()

  let set_connections (graph : t) (node : int) (neighbours : Neighbours.t) =
    (* printf "setting connections from %d to %s\n%!"
     *   node (Sexp.to_string_hum @@ Neighbours.sexp_of_t neighbours); *)
    let old_neighbours = Vector.get graph node in
    let added_neighbours, removed_neighbours = Neighbours.diff_both old_neighbours neighbours in
    (*  1. set the connections for the node  *)
    Vector.set graph node neighbours;
    (*  2. remove link to node from removed connections  *)
    Set.iter removed_neighbours 
      ~f:(fun removed_neighbour ->
          Neighbours.remove (Vector.get graph removed_neighbour) node);
    (*  3. add link to node to added connections  *)
    Set.iter added_neighbours 
      ~f:(fun added_neighbour ->
          Neighbours.add (Vector.get graph added_neighbour) node)

  module Test = struct
    let create_loop values =
      let n = Array.length values in
      let wrap i = if i < 0 then i+n else if i >= n then i-n else i in
      let g = create n in
      for i = 0 to n - 1 do
        set_connections g i (Neighbours.Test.of_list @@ List.map ~f:wrap [i-1; i+1])
      done;
      g

    let has_link g a b =
      Neighbours.Test.mem (adjacent g a) b

    let invariant g =
      let exception Not_symmetric in
      try
        iter_neighbours g ~f:(fun node neighbours ->
            if not @@ Neighbours.for_all neighbours ~f:(fun neighbour ->
                has_link g neighbour node) then
              raise Not_symmetric);
        true
      with Not_symmetric -> false

    let%test _ = num_nodes (empty ()) = 0
    let%test _ = num_nodes (create 42) = 42
    let%test _ = let g = create 42 in add_node g; num_nodes g = 43
    let%test _ = invariant (empty ())
    let%test _ = invariant (create 53)
    let%test _ = let g = create 12 in add_node g; invariant g
    let%test _ =
      let g = create 12 in
      add_node g; set_connections g 1 (Neighbours.Test.of_list [1;2;3;11]);
      has_link g 1 1 && has_link g 1 2 && has_link g 1 3 && has_link g 1 11
    let%test _ =
      let g = create 12 in
      add_node g; set_connections g 1 (Neighbours.Test.of_list [1;2;3;11]);
      invariant g
    let%test _ =
      let g = create 12 in
      add_node g;
      set_connections g 1 (Neighbours.Test.of_list [1;2;3;11]);
      set_connections g 1 (Neighbours.Test.of_list []);
      invariant g && not @@ has_link g 1 2

    let%test _ =
      let g = create_loop [|0.; 1.; 2.; 3.; 4.|] in
      invariant g
  end
end

(** A set of visited nodes.
*)
module Visited = struct
  type t = { visited : int array; mutable epoch : int } [@@deriving sexp]
  let create n = { visited = Array.create ~len:n 0; epoch = 1 }
  let mem visited node = visited.visited.(node) >= visited.epoch
  let add visited node = visited.visited.(node) <- visited.epoch
  let card (a : t) = Array.fold a.visited ~init:0 ~f:(fun acc e -> if e >= a.epoch then acc+1 else acc)
  let clear (visited : t) =
    if visited.epoch < Int.max_value - 1 then 
      visited.epoch <- visited.epoch + 1
    else begin
      Array.fill visited.visited ~pos:0 ~len:(Array.length visited.visited) 0;
      visited.epoch <- 1
    end

  module Test = struct
    let%test _ = card (create 42) = 0
    let%test _ = let v = create 42 in add v 41; add v 0; card v = 2
    let%test _ = let v = create 42 in add v 41; add v 0; mem v 41 && mem v 0
    let%test _ = let v = create 42 in add v 0; clear v; card v = 0 && not @@ mem v 0
    let%test _ = let v = create 3 in not (mem v 0) && not (mem v 1) && not (mem v 2)
    let%test _ = let v = create 3 in
      try
        let _ = mem v 3 in false
      with _ -> true
    let%test _ = let v = create 3 in add v 1; not (mem v 0) && mem v 1 && not (mem v 2)
    let%test _ = let v = create 3 in add v 1; add v 1;
      not (mem v 0) && mem v 1 && not (mem v 2) && card v = 1
    let%test _ = let v = create 3 in add v 1; clear v;
      not (mem v 0) && not (mem v 1) && not (mem v 2) && card v = 0
    let%test _ = let v = create 3 in
      v.epoch <- Int.max_value - 10;
      for i = 0 to 15 do
        add v 1;
        clear v;
        assert (not (mem v 1) && card v = 0);
        add v 1;
        clear v;
        assert (not (mem v 1) && card v = 0)
      done;
      true
  end
end

type 'a distance = 'a -> 'a -> float [@@deriving sexp]
type 'a value = int -> 'a [@@deriving sexp]

(** An imperative stack of graphs (layers).

    All layers have the same nodes (which are integers). This also holds
    a value function to compute a value associated with a node, and a
    distance function to compute a distance between two values.

*)
module Hgraph = struct
  type 'a t = {
    layers : Graph.t Vector.t;
    mutable entry_point : int option; (** entry node into max layer *)
    distance : 'a distance;
    value : 'a value
  } [@@deriving sexp]

  (*  needed? fold_layers  *)

  let create (distance : 'a distance) (value : 'a value) =
    let ret =
      { layers = Vector.create_with ~capacity:15 (Graph.empty ());
        entry_point = None;
        distance;
        value }
    in
    Vector.push_back ret.layers (Graph.empty ());
    ret

  let layer h i = Vector.get h.layers i

  let add_node (h : _ t) =
    Vector.iter h.layers ~f:Graph.add_node;
    Graph.num_nodes (layer h 0) - 1

  let distance h = h.distance
  let value h = h.value

  let num_nodes h = Graph.num_nodes @@ Vector.get h.layers 0

  let has_node h node =
    node >= 0 && node < num_nodes h

  let entry_point h = h.entry_point
  let set_entry_point h node =
    if not @@ has_node h node then
      invalid_arg "Hgraph.set_entry_point: invalid node";
    h.entry_point <- Some node

  let max_layer h = Vector.length h.layers - 1
  let set_max_layer h n =
    let num_nodes = Graph.num_nodes @@ Vector.get h.layers (Vector.length h.layers - 1) in
    for i = (max_layer h + 1) to n do
      Vector.push_back h.layers (Graph.create num_nodes)
    done

  module Test = struct
    let invariant h =
      Vector.for_all h.layers ~f:(fun g -> Graph.Test.invariant g) &&
      Vector.for_all h.layers ~f:(fun g -> Graph.num_nodes g = Graph.num_nodes (layer h 0)) &&
      (match entry_point h with
       | None -> true
       | Some e -> has_node h e)

    let distance a b = Float.abs (a -. b)
    let value values i = values.(i)

    let layer_exists h i = try let _ = layer h i in true; with _ -> false

    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      invariant h && num_nodes h = 0 && max_layer h = 0 && not @@ has_node h 0 && entry_point h = None
    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      let added = add_node h in
      set_entry_point h 0;
      invariant h &&
      added = 0 &&
      has_node h 0 &&
      (not @@ has_node h 1) &&
      entry_point h = Some 0
    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      let _ = add_node h in
      try set_entry_point h 1; false with _ -> true
    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      let _ = add_node h in
      invariant h && num_nodes h = 1 && max_layer h = 0
    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      let _ = add_node h in
      invariant h && (layer_exists h 0) && not @@ layer_exists h 1
    let%test _ =
      let h = create distance (value [|1.; 2.; 3.|]) in
      let a1 = add_node h in
      set_max_layer h 3;
      let a2 = add_node h in
      invariant h &&
      a1 = 0 && a2 = 1 &&
      List.for_all [0; 1; 2; 3] ~f:(layer_exists h) &&
      not @@ layer_exists h 4 &&
      num_nodes h = 2
  end
end

module HeapElt = struct
  type t = { node : int; distance : float } [@@deriving sexp]
  let create (distance : 'a distance) (value : 'a value) (target : 'a) node =
    { node; distance = distance target (value node) }
  let compare_nearest a b = Float.compare a.distance b.distance
  let compare_farthest a b = Float.compare b.distance a.distance
end

let search_one_paper
    (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (visited : Visited.t) (start_node : int) (target : 'a) =

  let heap_element = HeapElt.create distance value target in

  Visited.clear visited;
  Visited.add visited start_node;

  let visit_me = Heap.create ~cmp:HeapElt.compare_nearest () in
  Heap.add visit_me (heap_element start_node);

  let nearest = ref (heap_element start_node) in

  (* Format.printf "search_one on graph: @[%a@]@."
   *   Sexp.pp_hum ([%sexp_of : Graph.t] graph); *)

  let rec aux () =
    (* Format.printf "visit_me: @[%a@]@ visited: @[%a@]@ nearest: @[%a@]@."
     *   Sexp.pp_hum ([%sexp_of : HeapElt.t Heap.t] visit_me)
     *   Sexp.pp_hum ([%sexp_of : Visited.t] visited)
     *   Sexp.pp_hum ([%sexp_of : HeapElt.t] !nearest); *)
    match Heap.pop visit_me with
    | None -> !nearest.node
    | Some c ->
      (* Format.printf "  popped %a@."
       *   Sexp.pp_hum ([%sexp_of : HeapElt.t] c); *)
      if c.distance > !nearest.distance then !nearest.node
      else begin
        (* Format.printf "iterating on neighours: @[%a@]@."
         *   Sexp.pp_hum ([%sexp_of : Neighbours.t] @@ Graph.adjacent graph c.node); *)
        Neighbours.iter (Graph.adjacent graph c.node) ~f:(fun e ->
            (* Format.printf "  neighbour: %d@." e; *)
            if not @@ Visited.mem visited e then begin
              Visited.add visited e;
              let e = heap_element e in
              if e.distance < !nearest.distance then begin
                Heap.add visit_me e;
                nearest := e
              end
            end);
        aux ()
      end

  in aux ();;

(* Not following what is in the paper. We don't need a heap.  *)
let search_one_simple (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (visited : Visited.t) (start_node : int) (target : 'a) =
  let changed = ref true in
  let best_node = ref start_node in
  let best_distance = ref (distance (value start_node) target) in
  while !changed do
    changed := false;
    let neighbours = Graph.adjacent graph !best_node in
    Neighbours.iter neighbours ~f:(fun neighbour ->
        let d = distance (value neighbour) target in
        if d < !best_distance then begin
          best_node := neighbour;
          best_distance := d;
          changed := true
        end)
  done;
  !best_node

(* I can't measure a consistent difference between these. Using the
   simplest one seems reasonable. *)
let search_one = search_one_simple

module TestSearchOne = struct
  let%test "search_one in unconnected graph" =
    let graph = Graph.create 3 in 
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value [|1.; 2.; 3.|] in
    let visited = Visited.create (Graph.num_nodes graph) in
    search_one graph distance value visited 1 3.1 = 1 &&
    search_one graph distance value visited 1 ~-.42. = 1 &&
    search_one graph distance value visited 1 42. = 1

  let%test "search_one in connected graph" =
    let values = [|1.; 2.; 3.; 4.; 5.|] in
    let graph = Graph.Test.create_loop values in
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let visited = Visited.create (Graph.num_nodes graph) in
    search_one graph distance value visited 1 3.1 = 2 &&
    search_one graph distance value visited 1 ~-.0.5 = 0 &&
    search_one graph distance value visited 1 42. = 4 &&
    search_one graph distance value visited 1 1.6 = 1
end

(* destroys start_nodes (uses it as visit_me set) *)
let search_k
    (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (visited : Visited.t) (start_nodes : HeapElt.t Heap.t) (target : 'a) (k : int) =

  assert (not @@ Heap.is_empty start_nodes);
  assert (k > 0);
  let heap_element = HeapElt.create distance value target in

  Visited.clear visited;
  Heap.iter start_nodes ~f:(fun e -> Visited.add visited e.node);

  let visit_me = start_nodes in

  let nearest = Heap.create ~cmp:HeapElt.compare_farthest () in
  Heap.iter start_nodes ~f:(fun e -> Heap.add nearest e);

  let rec aux () =
    match Heap.pop visit_me with
    | None -> ()
    | Some c ->
      if c.distance > (Heap.top_exn nearest).distance then ()
      else begin
        Neighbours.iter (Graph.adjacent graph c.node) ~f:(fun e ->
            if not @@ Visited.mem visited e then begin
              Visited.add visited e;
              let e = heap_element e in
              if Heap.length nearest < k || e.distance < (Heap.top_exn nearest).distance then begin
                Heap.add visit_me e;
                Heap.add nearest e;
                if Heap.length nearest > k then ignore (Heap.pop_exn nearest)
              end
            end);
        aux ()
      end

  in
  aux ();
  assert (not @@ Heap.is_empty nearest);
  let ret = Heap.create ~cmp:HeapElt.compare_nearest () in
  Heap.iter nearest ~f:(Heap.add ret);
  (* Format.printf "search_k returns @[%a@]@."
   *   Sexp.pp_hum @@ [%sexp_of : HeapElt.t Heap.t] ret; *)
  ret;;

module TestSearchK = struct
  let%expect_test "search_k in unconnected graph" =
    let graph = Graph.create 3 in 
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value [|1.; 2.; 3.|] in
    let visited = Visited.create (Graph.num_nodes graph) in
    let init target start_node =
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      Heap.add h (HeapElt.create distance value target start_node);
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string @@ [%sexp_of: HeapElt.t Heap.t] x in
    pr @@ search_k graph distance value visited (init 3.1 1) 3.1 2;
    [%expect {| (((node 1)(distance 1.1))) |}];
    pr @@ search_k graph distance value visited (init 3.1 0) 3.1 1;
    [%expect {| (((node 0)(distance 2.1))) |}];
    pr @@ search_k graph distance value visited (init 0. 2) 0. 42;
    [%expect {| (((node 2)(distance 3))) |}]

  let%expect_test "search_k in connected graph" =
    let values = [|0.; 1.; 2.; 3.; 5.|] in
    let graph = Graph.Test.create_loop values in
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let visited = Visited.create (Graph.num_nodes graph) in
    let init target start_node =
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      Heap.add h (HeapElt.create distance value target start_node);
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string_hum @@ [%sexp_of: HeapElt.t Heap.t] x in
    pr @@ search_k graph distance value visited (init 4.5 1) 4.5 2;
    [%expect {| (((node 4) (distance 0.5)) ((node 3) (distance 1.5))) |}];
    pr @@ search_k graph distance value visited (init 1. 4) 1. 1;
    [%expect {| (((node 1) (distance 0))) |}];
    pr @@ search_k graph distance value visited (init 0. 2) 0. 3;
    [%expect {| (((node 0) (distance 0)) ((node 1) (distance 1)) ((node 2) (distance 2))) |}];
    pr @@ search_k graph distance value visited (init 0. 2) 0. 42;
    [%expect {|
      (((node 0) (distance 0)) ((node 1) (distance 1)) ((node 2) (distance 2))
       ((node 3) (distance 3)) ((node 4) (distance 5))) |}]
end

(*  destroys possible_neighbours_min_queue  *)
let select_neighbours (distance : 'a distance) (value : 'a value)
    (possible_neighbours_min_queue : HeapElt.t Heap.t) (num_neighbours : int) =
  assert (not @@ Heap.is_empty possible_neighbours_min_queue);
  (* printf "selecting %d neighbours from %s\n%!"
   *   num_neighbours (Sexp.to_string_hum @@ Heap.sexp_of_t HeapElt.sexp_of_t possible_neighbours_min_queue); *)
  let selected_neighbours = Neighbours.create () in
  let rec aux () =
    match Heap.pop possible_neighbours_min_queue with
    | None -> ()
    | Some e ->
      if Neighbours.for_all selected_neighbours
          ~f:(fun neighbour -> e.distance < distance (value neighbour) (value e.node)) then
        Neighbours.add selected_neighbours e.node;
      if Neighbours.length selected_neighbours < num_neighbours then aux ()
  in
  aux ();
  selected_neighbours;;

module TestSelectNeighbours = struct
  let%expect_test _ =
    let values = [|0.; 1.; 2.; 3.; 4.|] in
    (* let graph = Graph.Test.create_loop values in *)
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let candidates target =
      let element = HeapElt.create distance value in
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      Heap.add h (element target 0);
      Heap.add h (element target 1);
      Heap.add h (element target 3);
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string_hum @@ [%sexp_of: int List.t] @@ Neighbours.Test.to_sorted_list x in
    pr @@ select_neighbours distance value (candidates 1.) 3;
    [%expect {| (1) |}];
    pr @@ select_neighbours distance value (candidates 1.) 42;
    [%expect {| (1) |}];
    pr @@ select_neighbours distance value (candidates 1.4) 42;
    [%expect {| (1 3) |}];
    pr @@ select_neighbours distance value (candidates 0.6) 2;
    [%expect {| (0 1) |}];
    pr @@ select_neighbours distance value (candidates 0.6) 1;
    [%expect {| (1) |}]

  let%expect_test _ =
    let values = [|0.; 1.; 2.; 3.; 4.|] in
    (* let graph = Graph.Test.create_loop values in *)
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let target = 5. in
    let candidates list =
      let element = HeapElt.create distance value in
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      List.iter list ~f:(fun i -> Heap.add h (element target i));
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string_hum @@ [%sexp_of: int List.t] @@ Neighbours.Test.to_sorted_list x in
    pr @@ select_neighbours distance value (candidates [1;2;3;4]) 1;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4]) 2;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4]) 3;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4]) 4;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4]) 5;
    [%expect {| (4) |}]

  let%expect_test _ =
    let values = [|0.; 1.; 2.; 3.; 4.; 7.5|] in
    (* let graph = Graph.Test.create_loop values in *)
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let target = 5. in
    let candidates list =
      let element = HeapElt.create distance value in
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      List.iter list ~f:(fun i -> Heap.add h (element target i));
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string_hum @@ [%sexp_of: int List.t] @@ Neighbours.Test.to_sorted_list x in
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5]) 1;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5]) 2;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5]) 3;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5]) 4;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5]) 5;
    [%expect {| (4 5) |}]

  let%expect_test _ =
    let values = [|0.; 1.; 2.; 3.; 4.; 7.5; 10.|] in
    (* let graph = Graph.Test.create_loop values in *)
    let distance = Hgraph.Test.distance in
    let value = Hgraph.Test.value values in
    let target = 5. in
    let candidates list =
      let element = HeapElt.create distance value in
      let h = Heap.create ~cmp:HeapElt.compare_nearest () in
      List.iter list ~f:(fun i -> Heap.add h (element target i));
      h
    in
    let pr x = printf "%s\n" @@ Sexp.to_string_hum @@ [%sexp_of: int List.t] @@ Neighbours.Test.to_sorted_list x in
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5;6]) 1;
    [%expect {| (4) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5;6]) 2;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5;6]) 3;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5;6]) 4;
    [%expect {| (4 5) |}];
    pr @@ select_neighbours distance value (candidates [1;2;3;4;5;6]) 5;
    [%expect {| (4 5) |}]

end

let insert (hgraph : _ Hgraph.t) (target : 'a)
    ~(num_connections : int) (*  M  *)
    ~(num_nodes_search_construction : int) (*  efConstruction  *)
    (level_mult : float)
    (visited : Visited.t) =
  let new_node = Hgraph.add_node hgraph in

  match Hgraph.entry_point hgraph with
  | None ->
    (* XXX putting this here to fix init problem (first node added), not
       sure this is is right *)
    Hgraph.set_entry_point hgraph new_node;
    Hgraph.set_max_layer hgraph 0
  | Some entry_point -> begin
      Visited.clear visited;
      let level = Int.of_float @@ Float.round_nearest @@ -. (Float.log (Random.float 1.)) *. level_mult in
      (* printf "inserting from level %d\n%!" level; *)

      let node = ref entry_point in
      for layer = Hgraph.max_layer hgraph downto level+1 do
        node := search_one (Hgraph.layer hgraph layer) hgraph.distance hgraph.value visited !node target;
        (* printf "after search in layer %d, nearest node is %d\n%!"
         *   layer !node; *)
      done;

      let min_queue_of_neighbours node neighbours =
        let neighbour_queue =
          Heap.create ~min_size:(Neighbours.length neighbours) ~cmp:HeapElt.compare_nearest ()
        in
        Neighbours.iter neighbours ~f:(fun neighbour ->
            Heap.add neighbour_queue (HeapElt.create hgraph.distance hgraph.value (hgraph.value node) neighbour));
        neighbour_queue
      in

      let w_queue = ref @@ Heap.create ~min_size:num_nodes_search_construction ~cmp:HeapElt.compare_nearest () in
      Heap.add !w_queue (HeapElt.create hgraph.distance hgraph.value target !node);

      for layer = Int.min level (Hgraph.max_layer hgraph) downto 0 do
        (* printf "-> inserting on layer %d\n%!" layer; *)
        let graph = (Hgraph.layer hgraph layer) in
        w_queue :=
          search_k graph hgraph.distance hgraph.value visited !w_queue target num_nodes_search_construction;
        let num_connections = if layer = 0 then 2 * num_connections else num_connections in
        let neighbours = select_neighbours hgraph.distance hgraph.value (Heap.copy !w_queue) num_connections in
        Graph.set_connections graph new_node neighbours;
        Neighbours.iter neighbours ~f:(fun neighbour ->
            let nn = Graph.adjacent graph neighbour in
            if Neighbours.length nn > num_connections then begin
              let neighbour_queue = min_queue_of_neighbours neighbour nn in
              let reduced_neighbours =
                select_neighbours hgraph.distance hgraph.value neighbour_queue num_connections
              in
              Graph.set_connections graph neighbour reduced_neighbours
            end);
      done;

      if level > Hgraph.max_layer hgraph then begin
        Hgraph.set_max_layer hgraph level;
        Hgraph.set_entry_point hgraph new_node
      end
    end


let build_batch_bigarray (distance : 'a distance) (batch : Lacaml.S.mat)
    ~num_connections ~num_nodes_search_construction =
  let value i = Lacaml.S.Mat.col batch (i+1) in
  let hgraph = Hgraph.create distance value in
  let level_mult = 1. /. Float.log (Float.of_int num_connections) in
  let visited = Visited.create (Lacaml.S.Mat.dim2 batch) in
  let n = Lacaml.S.Mat.dim2 batch in
  let k_print = n / 100 in
  let _ = Lacaml.S.Mat.fold_cols (fun i col ->
      if i % k_print = 0 then begin
        printf "\r              \r%d/%d %d%%%!" i n
          (Int.of_float (100. *. Float.of_int i /. Float.of_int n));
      end;
      insert hgraph col ~num_connections ~num_nodes_search_construction level_mult visited;
      i + 1
    ) 0 batch
  in
  hgraph;;

let knn (hgraph : _ Hgraph.t) (visited : Visited.t)
    ~k (target : 'a) =
  match Hgraph.entry_point hgraph with
  | None -> invalid_arg "knn: empty hgraph"
  | Some entry_point ->
    let node = ref entry_point  in
    for layer = Hgraph.max_layer hgraph downto 1 do
      node := search_one (Hgraph.layer hgraph layer) hgraph.distance hgraph.value visited !node target
    done;
    let w_queue = Heap.create ~min_size:k ~cmp:HeapElt.compare_nearest () in
    Heap.add w_queue (HeapElt.create hgraph.distance hgraph.value target !node);
    search_k (Hgraph.layer hgraph 0) hgraph.distance hgraph.value visited w_queue target k

let knn_batch_bigarray (hgraph : _ Hgraph.t) ~k (batch : Lacaml.S.mat) =
  let size_batch = Lacaml.S.Mat.dim2 batch in
  let distances = Lacaml.S.Mat.create k size_batch in
  Lacaml.S.Mat.fill distances Float.nan;
  let ids = Array.init size_batch ~f:(fun i -> Array.create ~len:k (-1)) in
  let visited = Visited.create (Graph.num_nodes (Hgraph.layer hgraph 0)) in
  let _ = Lacaml.S.Mat.fold_cols (fun j col ->
      let nearest = knn hgraph visited ~k col in
      (* printf "nearest queue: %s\n%!" @@ Sexp.to_string_hum @@ Heap.sexp_of_t HeapElt.sexp_of_t nearest; *)
      let rec aux i = match Heap.pop nearest with
        | None -> ()
        | Some (e : HeapElt.t) ->
          distances.{i,j} <- e.distance;
          ids.(j-1).(i-1) <- e.node;
          aux (i+1)
      in
      aux 1;
      j+1
    ) 1 batch
  in
  ids, distances

let distance_l2 a b = Float.sqrt @@ Lacaml.S.Vec.ssqr_diff a b

(* TODO:

   - pass wqueue to search_* functions so that they can fill it,
   rather than having them reallocate it each time? not sure, NO

   - put distance and value inside hgraph? distance at least? DONE

   - check that heaps are min or max as needed

   - check that Visited is cleared every time it is needed

   - make knn return a list of index * distance instead of a heap?

   - have search_k take a single object including target? it is
   error-prone to pass the heap that refers to the target, and the
   target again

   - Have a MinQueue and MaxQueue wrapping Heap? This would ensure the
   right types are passed.
*)
