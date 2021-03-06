open Core_kernel
open Stdio

module Ohnsw = Hnsw.Ohnsw

let show_hgraph_inside chan hgraph =
  let module Hgraph = Ohnsw.Hgraph in
  let pf fmt = Out_channel.fprintf chan fmt in

  let node_name ~layer_index node =
    Printf.sprintf "%d/%d" layer_index node
  in

  let color_scheme =
    let n = Hgraph.max_layer hgraph + 1 in
    let m = if n <= 4 then 4
      else if n <= 6 then 6
      else if n <= 8 then 8
      else 10
    in Printf.sprintf "piyg%d" m
  in

  let show_node ~layer_index index =
    let pos = Hgraph.value hgraph index in
    let name = node_name ~layer_index index in
    let offset = Float.of_int layer_index *. 6. in
    pf "\"%s\" [\nlabel=\"%d\"\npos=\"%f,%f!\"\ncolor=\"/%s/%d\"\n]\n"
      name index (pos.{1}+.offset) (pos.{2}) color_scheme (layer_index+1)
  in

  let show_connection ~layer_index na nb =
    let name_a = node_name ~layer_index na in
    let name_b = node_name ~layer_index nb in
    pf "\"%s\" -- \"%s\" [\ncolor=\"/%s/%d\"\n]\n"
      name_a name_b color_scheme (layer_index+1)
  in

  let show_layer layer_index (layer : Ohnsw.Graph.t) =
    Ohnsw.Graph.iter_neighbours layer ~f:(fun node neighbours ->
        show_node ~layer_index node;
        Ohnsw.Neighbours.iter neighbours ~f:(fun neighbour ->
            show_connection ~layer_index node neighbour)
      )
  in

  for i = 0 to Hgraph.max_layer hgraph do
    show_layer i (Hgraph.layer hgraph i)
  done;;


let show_hgraph hgraph filename =
  Out_channel.with_file filename ~f:(fun chan ->
      let pf fmt = Out_channel.fprintf chan fmt in
      pf "graph g {\n";
      show_hgraph_inside chan hgraph;
      pf "}\n")

let show_neighbours hgraph point neighbours filename =
  let module Hgraph = Ohnsw.Hgraph in
  Out_channel.with_file filename ~f:(fun chan ->
      let pf fmt = Out_channel.fprintf chan fmt in
      pf "graph g {\n";
      show_hgraph_inside chan hgraph;
      pf "\"point\" [ pos=\"%f,%f!\" ]\n"
        point.{1} point.{2};
     let _ = Heap.fold neighbours ~init:0 ~f:(fun i (e : Ohnsw.HeapElt.t) ->
          let pos = Hgraph.value hgraph e.node in
          pf "\"n%d\" [ pos=\"%f,%f!\" ]\n"
            i pos.{1} pos.{2};
          i+1
        )
     in
     let neighbours = Heap.to_list neighbours in
     List.iteri neighbours ~f:(fun i n ->
         let pos = Hgraph.value hgraph n.node in
         pf "\"n%d\" [ pos=\"%f,%f!\" ]\n"
           i pos.{1} pos.{2};
       );
     pf "}\n")

let random_data dim n =
  Lacaml.S.Mat.random ~range:6. dim n
(* List.init n ~f:(fun _ -> Array.init dim ~f:(fun _ -> Random.float 100.)) *)

let shuffle ret =
  Lacaml.S.Mat.to_col_vecs_list ret |> List.permute |> Lacaml.S.Mat.of_col_vecs_list

let grid_data n =
  let ret = Lacaml.S.Mat.make 2 n 0. in
  let sn = Int.of_float @@ Float.round_up @@ Float.sqrt (Float.of_int n) in
  let k = ref 0 in
  for i = 0 to sn - 1 do
    for j = 0 to sn - 1 do
      Int.incr k;
      if !k <= Lacaml.S.Mat.dim2 ret then begin
        ret.{1, !k} <- Float.of_int i;
        ret.{2, !k} <- Float.of_int j
      end
    done
  done;
  shuffle ret

type vec = Lacaml.S.vec sexp_opaque [@@deriving sexp]

let test_graphical () = ();;
let dim = 2 in
let n_train = 36 in
let n_test = 10 in
(* let train_data = random_data dim n_train in *)
let train_data = grid_data n_train in
let test_data = random_data dim n_test in
Caml.Format.printf "train data:\n%a\n" Lacaml.S.pp_mat train_data;
Caml.Format.printf "test data:\n%a\n" Lacaml.S.pp_mat test_data;
let hgraph = Ohnsw.build_batch_bigarray Ohnsw.distance_l2
    ~num_connections:3 ~num_nodes_search_construction:20 train_data
in
printf "graph:\n%s\n" (Sexp.to_string_hum @@ Ohnsw.Hgraph.sexp_of_t sexp_of_vec hgraph);
show_hgraph hgraph "hgraph.dot";
let i = ref 0 in
Lacaml.S.Mat.fold_cols (fun () point ->
    let visited = Ohnsw.Visited.create (Ohnsw.Graph.num_nodes (Ohnsw.Hgraph.layer hgraph 0)) in
    let neighbours = Ohnsw.knn hgraph visited point ~k:3 in
    Caml.Format.printf "neighbours of %a:\n%s\n"
      Lacaml.S.pp_vec point
      (Sexp.to_string_hum @@ [%sexp_of : Ohnsw.HeapElt.t Heap.t] neighbours);
    show_neighbours hgraph point neighbours (Printf.sprintf "neighbours_%03d.dot" !i);
    Int.incr i) () test_data;;
(*  TODO:
    - print to dot
    - write simple (expect?) tests
    - run on benchmark
*)

test_graphical ();;
