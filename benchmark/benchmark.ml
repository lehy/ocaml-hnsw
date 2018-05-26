open Base
open Stdio
open Hdf5_caml

module Recall = Dataset.Recall
module Dataset = Dataset.Dataset
module Ohnsw = Hnsw.Ohnsw                 

(* let read_attribute_string t name = *)
(*   let open Hdf5_caml in *)
(*   let open Hdf5_raw in *)
(*   (\* let att = H5a.open_ (H5.hid t) name in *\) *)
(*   let att = H5a.open_name (H5.hid t) name in *)
(*   let dataspace = H5a.get_space att in *)
(*   let datatype = H5a.get_type att in *)
(*   let buf = *)
(*     if H5t.is_variable_str datatype then begin *)
(*       (\* let space = H5a.get_space att in *\) *)
(*       let dims, toto = H5s.get_simple_extent_dims dataspace in *)
(*       printf "variable-length string dims: %s %s\n%!" *)
(*         (Sexp.to_string_hum @@ [%sexp_of : int array] @@ dims) *)
(*         (Sexp.to_string_hum @@ [%sexp_of : int array] @@ toto) *)
(*       ; *)
(*       let a = Bytes.create 12 (\* dims.(0) *\) in *)

(*       let memtype = H5t.copy H5t.c_s1 in *)
(*       H5t.set_size memtype H5t.variable; *)
(*       H5a.read_string att memtype a; *)
(*         (\* let xfer_plist = H5p.create H5p.Cls_id.DATASET_XFER in *\) *)
(*         (\* H5p.set_vlen_mem_manager xfer_plist (fun i -> *\) *)
(*         (\*     printf "allocating %d bytes\n%!" i; *\) *)
(*         (\*     Bytes.create (i - 1)) *\) *)
(*         (\*   (fun s -> *\) *)
(*         (\*      printf "deallocating: '%s'\n%!" (Bytes.to_string s)); *\) *)
(*         (\* H5d.read_string att datatype H5s.all H5s.all ~xfer_plist (Bytes.unsafe_to_string a); *\) *)
(*       a *)
(*     end else begin *)
(*       let size = H5t.get_size datatype in *)
(*       printf "string size: %d\n" size; *)
(*       let a = Bytes.create size in *)
(*       H5a.read_string att datatype a; *)
(*       a *)
(*     end *)
(*   in *)
(*   H5t.close datatype; *)
(*   H5s.close dataspace; *)
(*   H5a.close att; *)
(*   Bytes.unsafe_to_string buf *)

let nans_like a =
  let ret = Bigarray.Array2.create (Bigarray.Array2.kind a)
      Bigarray.fortran_layout (Bigarray.Array2.dim1 a) (Bigarray.Array2.dim2 a)
  in
  Bigarray.Array2.fill ret Float.nan;
  ret

let read_data ?limit_train ?limit_test () =
  let data = Dataset.read "fashion-mnist-784-euclidean.hdf5" ?limit_train ?limit_test in
  (* let data = Dataset.random ~num_train:limit_train ~num_test:limit_test ~k:100 ~dim:784 in *)
  printf "read dataset: %s\n" (Sexp.to_string_hum @@ Dataset.sexp_of_t data);
  data

let random_data ~num_train ~num_test ~k ~dim =
  Dataset.random ~num_train ~num_test ~k ~dim

let build_index ?(num_neighbours=5) ?(num_neighbours_build=10) data =
  let t0 = Unix.gettimeofday () in
  let hgraph = Hnsw.Ba.build ~num_neighbours ~num_neighbours_build data.Dataset.train in
  let t1 = Unix.gettimeofday () in
  printf "index construction: %f s\n%!" (t1-.t0);
  let stats = Hnsw.Ba.Hgraph.Stats.compute hgraph in
  printf "index stats: %s\n%!" (Sexp.to_string_hum @@ Hnsw.Ba.Hgraph.Stats.sexp_of_t stats);
  hgraph

let build_index_new ?(num_neighbours=5) ?(num_neighbours_build=10) (data : Dataset.t) =
  let t0 = Unix.gettimeofday () in
  let hgraph = Ohnsw.build_batch_bigarray Ohnsw.distance_l2 data.train
      ~num_connections:num_neighbours ~num_nodes_search_construction:num_neighbours_build
  in
  let t1 = Unix.gettimeofday () in
  printf "index construction: %f s\n%!" (t1-.t0);
  (* let stats = Hnsw.Ba.Hgraph.Stats.compute hgraph in
   * printf "index stats: %s\n%!" (Sexp.to_string_hum @@ Hnsw.Ba.Hgraph.Stats.sexp_of_t stats); *)
  (* printf "hgraph:\n%s\n%!" @@ Sexp.to_string_hum @@ Ohnsw.Hgraph.sexp_of_t hgraph; *)
  hgraph

let test_new (data : Dataset.t) hgraph =
  let num_neighbours = Bigarray.Array2.dim1 data.test_distances in
  printf "bench: num_neighbours: %d\n" num_neighbours;
  let t1 = Unix.gettimeofday () in
  let got_ids, got_distances =
    Ohnsw.knn_batch_bigarray hgraph data.test ~k:num_neighbours
  in
  let t2 = Unix.gettimeofday () in
  let num_queries = Bigarray.Array2.dim2 data.test |> Float.of_int in
  printf "query: %f s, %f q/s, %f s/q\n%!"
    (t2-.t1) (num_queries/.(t2-.t1)) ((t2-.t1)/.num_queries);
  let recall = Recall.compute data.test_distances got_distances in
  printf "recall: %f\n" recall;;

let test (data : Dataset.t) hgraph =
  let num_neighbours = Bigarray.Array2.dim1 data.test_distances in
  printf "bench: num_neighbours: %d\n" num_neighbours;
  let t1 = Unix.gettimeofday () in
  let got_distances =
    Hnsw.Ba.knn_batch hgraph data.test ~num_neighbours ~num_neighbours_search:num_neighbours
  in
  let t2 = Unix.gettimeofday () in
  let num_queries = Bigarray.Array2.dim2 data.test |> Float.of_int in
  printf "query: %f s, %f q/s, %f s/q\n%!"
    (t2-.t1) (num_queries/.(t2-.t1)) ((t2-.t1)/.num_queries);
  let recall = Recall.compute data.test_distances got_distances in
  printf "recall: %f\n" recall;;


let main () =
  (* let data = read_data ~limit_train:10000 ~limit_test:10 () in *)
  let data = random_data ~num_train:10000 ~num_test:10 ~dim:784 ~k:10 in
  let index_only = Array.length Caml.Sys.argv > 2 in
  match Caml.Sys.argv.(1) with
  | "old" ->
    let hgraph = build_index data ~num_neighbours:7 ~num_neighbours_build:100 in
    if not index_only then test data hgraph
  | "new" ->
    let hgraph = build_index_new data ~num_neighbours:7 ~num_neighbours_build:100 in
    if not index_only then test_new data hgraph
  | _ -> failwith "usage: benchmark old|new [index]";;

main ();;

(*  TODO:
    - optimize index creation (Nearest.fold to MinHeap: pass MinHeap directly to SelectNeighbours)
    - find a way to profile the thing:
    + perf record / perf report seem to work!
    + hotpoints:
      - Map.find (probably values + neighbours) -> convert values to just our Bigarray
      - Set.fold -> convert sets to lists?
    + try to benchmark index creation and querying separately

new TODO:
- l2 distance computation is on par with what C++ does (even a bit better!)
- simple random tests show that our recall is very weak (like 1/2% even with num neighbours build = 400: investigate

*)
