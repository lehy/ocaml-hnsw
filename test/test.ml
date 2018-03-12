open Base
open Stdio

let show_hgraph hgraph filename = ()
let show_neighbours hgraph point neighbours filename = ()

let random_data dim n =
  List.init n ~f:(fun _ -> Array.init dim ~f:(fun _ -> Random.float 100.))

let test_graphical () =
  let dim = 2 in
  let n_train = 20 in
  let n_test = 5 in
  let train_data = random_data dim n_train in
  let test_data = random_data dim n_test in
  printf "train data:\n%s\n" (Sexp.to_string_hum @@ [%sexp_of : float array list] train_data);
  printf "test data:\n%s\n" (Sexp.to_string_hum @@ [%sexp_of : float array list] test_data);
  let hgraph = Hnsw.build_array (List.fold_left train_data) in
  printf "graph:\n%s\n" (Sexp.to_string_hum @@ Hnsw.HgraphEuclideanArray.sexp_of_t hgraph);
  show_hgraph hgraph "hgraph.svg";
  let i = ref 0 in
  List.iter test_data ~f:(fun point ->
      let neighbours = Hnsw.knn_array hgraph point 3 in
      printf "neighbours of %s:\n%s\n"
        (Sexp.to_string_hum @@ [%sexp_of : float array] point)
        (Sexp.to_string_hum @@ [%sexp_of : Hnsw.KnnArray.MinHeap.Element.t list] neighbours);
      show_neighbours hgraph point neighbours (Printf.sprintf "neighbours_%03d.svg" !i);
      Int.incr i);;
(*  TODO:
- print to dot
- write simple (expect?) tests
- run on benchmark
*)

test_graphical ();;
