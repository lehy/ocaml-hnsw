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

  let fold v ~init ~f =
    let acc = ref init in
    for i = 0 to v.size - 1 do
      acc := f !acc v.buf.(i)
    done;
    !acc         

  let length v = v.size
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

  let%test _ = length (create 0) = 0
  let%test _ = length (create_with ~capacity:56 0) = 0
  let%test _ = capacity (create_with ~capacity:56 0) = 56
  let%test _ = let a = create 0 in push_back a 42; length a = 1
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
    printf "setting connections from %d to %s\n%!"
      node (Sexp.to_string_hum @@ Neighbours.sexp_of_t neighbours);
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
end

module Visited = struct
  type t = { visited : int array; mutable epoch : int } [@@deriving sexp]
  let create n = { visited = Array.create ~len:n 0; epoch = 1 }
  let mem visited node = visited.visited.(node) >= visited.epoch
  let add visited node = visited.visited.(node) <- visited.epoch
  let card a = Array.fold a.visited ~init:0 ~f:(fun acc e -> if mem a e then acc+1 else acc)
  let clear (visited : t) =
    if visited.epoch < Int.max_value - 1 then 
      visited.epoch <- visited.epoch + 1
    else begin
      Array.fill visited.visited ~pos:0 ~len:(Array.length visited.visited) 0;
      visited.epoch <- 1
    end
end

type 'a distance = 'a -> 'a -> float [@@deriving sexp]
type 'a value = int -> 'a [@@deriving sexp]

(** An imperative stack of graphs (layers). *)
module Hgraph = struct
  type 'a t = {
    layers : Graph.t Vector.t;
    mutable entry_point : int; (** entry node into max layer *)
    distance : 'a distance;
    value : 'a value
  } [@@deriving sexp]

  (*  needed? fold_layers  *)

  let create (distance : 'a distance) (value : 'a value) =
    let ret =
      { layers = Vector.create_with ~capacity:15 (Graph.empty ());
        entry_point = -1;
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

  let entry_point h = h.entry_point
  let set_entry_point h node = h.entry_point <- node

  let max_layer h = Vector.length h.layers - 1
  let set_max_layer h n =
    let num_nodes = Graph.num_nodes @@ Vector.get h.layers (Vector.length h.layers - 1) in
    for i = (max_layer h + 1) to n do
      Vector.push_back h.layers (Graph.create num_nodes)
    done
end

module HeapElt = struct
  type t = { node : int; distance : float } [@@deriving sexp]
  let create (distance : 'a distance) (value : 'a value) (target : 'a) node =
    { node; distance = distance target (value node) }
  let compare_nearest a b = Float.compare a.distance b.distance
  let compare_farthest a b = Float.compare b.distance a.distance
end

let search_one
    (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (visited : Visited.t) (start_node : int) (target : 'a) =

  let heap_element = HeapElt.create distance value target in

  Visited.clear visited;
  Visited.add visited start_node;

  let visit_me = Heap.create ~cmp:HeapElt.compare_nearest () in
  Heap.add visit_me (heap_element start_node);

  let nearest = ref (heap_element start_node) in

  let rec aux () =
    match Heap.pop visit_me with
    | None -> !nearest.node
    | Some c ->
      if c.distance > !nearest.distance then !nearest.node
      else begin
        Neighbours.iter (Graph.adjacent graph c.node) ~f:(fun e ->
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

(* destroys start_nodes (uses it as visit_me set) *)
let search_k
    (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (visited : Visited.t) (start_nodes : HeapElt.t Heap.t) (target : 'a) (k : int) =

  let heap_element = HeapElt.create distance value target in

  Visited.clear visited;
  Heap.iter start_nodes ~f:(fun e -> Visited.add visited e.node);

  let visit_me = start_nodes in

  let nearest = Heap.create ~cmp:HeapElt.compare_farthest () in
  Heap.iter start_nodes ~f:(fun e -> Heap.add nearest e);

  let rec aux () =
    match Heap.pop visit_me with
    | None -> nearest
    | Some c ->
      if c.distance > (Heap.top_exn nearest).distance then nearest
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
  let ret = Heap.create ~cmp:HeapElt.compare_nearest () in
  Heap.iter (aux ()) ~f:(Heap.add ret);
  ret;;

(*  destroys possible_neighbours_min_queue  *)
let select_neighbours (graph : Graph.t) (distance : 'a distance) (value : 'a value)
    (possible_neighbours_min_queue : HeapElt.t Heap.t) (num_neighbours : int) =
  printf "selecting %d neighbours from %s\n%!"
    num_neighbours (Sexp.to_string_hum @@ Heap.sexp_of_t HeapElt.sexp_of_t possible_neighbours_min_queue);
  let selected_neighbours = Neighbours.create () in
  if Heap.length possible_neighbours_min_queue <= num_neighbours then begin
    (* shortcut: if we already have the right number of neighbours, just return them *)
    Heap.iter possible_neighbours_min_queue ~f:(fun e -> Neighbours.add selected_neighbours e.node)
  end else begin let rec aux () =
                   match Heap.pop possible_neighbours_min_queue with
                   | None -> ()
                   | Some e ->
                     if Neighbours.for_all selected_neighbours
                         ~f:(fun neighbour -> e.distance < distance (value neighbour) (value e.node)) then
                       Neighbours.add selected_neighbours e.node;
                     if Neighbours.length selected_neighbours <= num_neighbours then aux ()
    in aux ();
  end;
  selected_neighbours;;

let insert (hgraph : _ Hgraph.t) (target : 'a)
    ~(num_connections : int) (*  M  *)
    ~(num_nodes_search_construction : int) (*  efConstruction  *)
    (level_mult : float)
    (visited : Visited.t) =
  let new_node = Hgraph.add_node hgraph in

  if Hgraph.entry_point hgraph < 0 then begin
    (* XXX putting this here to fix init problem (first node added), not
       sure this is is right *)
    Hgraph.set_entry_point hgraph new_node;
    Hgraph.set_max_layer hgraph 0
  end else begin
    Visited.clear visited;
    let level = Int.of_float @@ Float.round_nearest @@ -. (Float.log (Random.float 1.)) *. level_mult in
    printf "inserting from level %d\n%!" level;

    let node = ref @@ Hgraph.entry_point hgraph in
    for layer = Hgraph.max_layer hgraph downto level+1 do
      node := search_one (Hgraph.layer hgraph layer) hgraph.distance hgraph.value visited !node target;
      printf "after search in layer %d, nearest node is %d\n%!"
        layer !node;
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
      printf "-> inserting on layer %d\n%!" layer;
      let graph = (Hgraph.layer hgraph layer) in
      w_queue :=
        search_k graph hgraph.distance hgraph.value visited !w_queue target num_nodes_search_construction;
      let num_connections = if layer = 0 then 2 * num_connections else num_connections in
      let neighbours = select_neighbours graph hgraph.distance hgraph.value !w_queue num_connections in
      Graph.set_connections graph new_node neighbours;
      Neighbours.iter neighbours ~f:(fun neighbour ->
          let nn = Graph.adjacent graph neighbour in
          if Neighbours.length nn > num_connections then begin
            let neighbour_queue = min_queue_of_neighbours neighbour nn in
            let reduced_neighbours =
              select_neighbours graph hgraph.distance hgraph.value neighbour_queue num_connections
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
  Lacaml.S.Mat.fold_cols (fun () col ->
      insert hgraph col ~num_connections ~num_nodes_search_construction level_mult visited
    ) () batch;
  hgraph;;

let knn (hgraph : _ Hgraph.t) (visited : Visited.t)
    ~k (target : 'a) =
  let node = ref @@ Hgraph.entry_point hgraph in
  for layer = Hgraph.max_layer hgraph downto 1 do
    node := search_one (Hgraph.layer hgraph layer) hgraph.distance hgraph.value visited !node target
  done;
  let w_queue = Heap.create ~min_size:k ~cmp:HeapElt.compare_nearest () in
  Heap.add w_queue (HeapElt.create hgraph.distance hgraph.value target !node);
  search_k (Hgraph.layer hgraph 0) hgraph.distance hgraph.value visited w_queue target k

(* XXX TODO make hgraph remember value instead of repassing the
   training batch each time *)
let knn_batch_bigarray (hgraph : _ Hgraph.t) ~k (batch : Lacaml.S.mat) =
  let size_batch = Lacaml.S.Mat.dim2 batch in
  let distances = Lacaml.S.Mat.create k size_batch in
  Lacaml.S.Mat.fill distances Float.nan;
  let ids = Array.init size_batch ~f:(fun i -> Array.create ~len:k (-1)) in
  let visited = Visited.create (Graph.num_nodes (Hgraph.layer hgraph 0)) in
  let _ = Lacaml.S.Mat.fold_cols (fun j col ->
      let nearest = knn hgraph visited ~k col in
      printf "nearest queue: %s\n%!" @@ Sexp.to_string_hum @@ Heap.sexp_of_t HeapElt.sexp_of_t nearest;
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

   - pass wqueue to search_* functions so that they can fill it, rather
   than having them reallocate it each time? not sure, NO

   - put distance and value inside hgraph? distance at least? DONE

   - check that heaps are min or max as needed

   - check that Visited is cleared every time it is needed

   - make knn return a list of index * distance instead of a heap?
*)
