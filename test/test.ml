open Base
open Stdio

let show_hgraph hgraph filename = ()
let show_neighbours hgraph point neighbours filename = ()

let random_data dim n =
  Lacaml.S.Mat.random ~range:100. dim n
(* List.init n ~f:(fun _ -> Array.init dim ~f:(fun _ -> Random.float 100.)) *)

let test_graphical () = ();;
let dim = 2 in
let n_train = 20 in
let n_test = 5 in
let train_data = random_data dim n_train in
let test_data = random_data dim n_test in
Caml.Format.printf "train data:\n%a\n" Lacaml.S.pp_mat train_data;
Caml.Format.printf "test data:\n%a\n" Lacaml.S.pp_mat test_data;
let hgraph = Hnsw.Ba.build_batch train_data in
printf "graph:\n%s\n" (Sexp.to_string_hum @@ Hnsw.Ba.Hgraph.sexp_of_t hgraph);
show_hgraph hgraph "hgraph.svg";
let i = ref 0 in
Lacaml.S.Mat.fold_cols (fun () point ->
    let neighbours = Hnsw.Ba.knn hgraph point ~num_neighbours:3 ~num_neighbours_search:2 in
    Caml.Format.printf "neighbours of %a:\n%s\n"
      Lacaml.S.pp_vec point
      (Sexp.to_string_hum @@ [%sexp_of : Hnsw.Ba.Hgraph.node Hnsw.value_distance list] neighbours);
    show_neighbours hgraph point neighbours (Printf.sprintf "neighbours_%03d.svg" !i);
    Int.incr i) () test_data;;
(*  TODO:
    - print to dot
    - write simple (expect?) tests
    - run on benchmark
*)

test_graphical ();;
