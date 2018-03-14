open Base
open Stdio
open Hdf5_caml

module Distance = struct
  type t =
    | Euclidean
    | Unknown of string [@@deriving sexp]
  let of_string = function
    | "euclidean" -> Euclidean
    | x -> Unknown x
end

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


module Dataset = struct
  type t = {
    (*  train vectors  *)
    train : Lacaml.S.mat; (* (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array2.t; *)

    (*  test vectors  *)
    test : Lacaml.S.mat; (* (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array2.t; *)
    
    (*  for each test vector, distances of the true n nearest neighbours *)
    test_distances : Lacaml.S.mat; (* (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array2.t; *)

    (*  metric for comparing vectors  *)
    distance : Distance.t
  }

  module Sexp_of_t = struct
    type dataset = t
    type t = { train : int * int;
               test : int * int;
               test_distances : int * int;
               distance : Distance.t } [@@deriving sexp]
    let of_dataset (d : dataset) =
      { train=(Bigarray.Array2.dim1 d.train, Bigarray.Array2.dim2 d.train);
        test=(Bigarray.Array2.dim1 d.test, Bigarray.Array2.dim2 d.test);
        test_distances=(Bigarray.Array2.dim1 d.test_distances, Bigarray.Array2.dim2 d.test_distances);
        distance=d.distance
      }
  end
  
  let sexp_of_t t = Sexp_of_t.of_dataset t |> Sexp_of_t.sexp_of_t
  
  let read f =
    let data = H5.open_rdonly ("../../../data/" ^ f) in
    (* printf "%s: %s\n" f (Sexp.to_string_hum @@ [%sexp_of : string list] @@ H5.ls data); *)
    let to_lacaml x = Bigarray.Array2.change_layout x Bigarray.fortran_layout in
    let distance = H5.read_attribute_string data "distance" in
    let train = H5.Float32.read_float_array2 data "train" Bigarray.c_layout |> to_lacaml in
    let test = H5.Float32.read_float_array2 data "test" Bigarray.c_layout |> to_lacaml in
    let test_distances = H5.Float32.read_float_array2 data "distances" Bigarray.c_layout |> to_lacaml in
    (* printf "  train:%dx%d test: %dx%d distance: %s\n" *)
    (*   (Bigarray.Array2.dim1 train) *)
    (*   (Bigarray.Array2.dim2 train) *)
    (*   (Bigarray.Array2.dim1 test) *)
    (*   (Bigarray.Array2.dim2 test) *)
    (*   distance; *)
    { train; test; test_distances; distance=Distance.of_string distance }
  ;;
end;;

let nans_like a =
  let ret = Bigarray.Array2.create (Bigarray.Array2.kind a)
      Bigarray.fortran_layout (Bigarray.Array2.dim1 a) (Bigarray.Array2.dim2 a)
  in
  Bigarray.Array2.fill ret Float.nan;
  ret

module Recall = struct
  let compute ?(epsilon=1e-8) expected got =
    let module A = Bigarray.Array2 in
    if A.dim1 expected <> A.dim1 got || A.dim2 expected <> A.dim2 got then
      invalid_arg "Recall.compute: arrrays have unequal shapes";
    let num_queries = A.dim2 expected in
    let num_neighbours = A.dim1 expected in
    let ret = ref 0. in
    for i_query = 1 to num_queries do
      let num_ok = ref 0 in
      for i_neighbour = 1 to num_neighbours do
        if Float.( <= ) got.{i_neighbour, i_query} (expected.{num_neighbours, i_query} +. epsilon) then
          Int.incr num_ok
      done;
      ret := !ret +. ((Float.of_int !num_ok) /. (Float.of_int num_neighbours))
    done;
    !ret /. (Float.of_int num_queries)
end

let main () =
  let data = Dataset.read "fashion-mnist-784-euclidean.hdf5" in
  printf "read dataset: %s\n" (Sexp.to_string_hum @@ Dataset.sexp_of_t data);
  (* let fold_cols ~init ~f ba = *)
  (*   let n = Bigarray.Array2.dim2 ba in *)
  (*   let k_print = n / 100 in *)
  (*   let acc = ref init in *)
  (*   for i = 1 to n do *)
  (*     if i % k_print = 0 then begin *)
  (*       printf "\r              \r%d/%d %d%%%!" i n *)
  (*         (Int.of_float (100. *. Float.of_int i /. Float.of_int n)); *)
  (*     end; *)
  (*     let row = Lacaml.S.Mat.col ba i in *)
  (*     acc := f !acc row *)
  (*   done; *)
  (*   printf "\n"; *)
  (*   !acc *)
  (* in *)
  let t0 = Unix.gettimeofday () in
  let hgraph = Hnsw.Ba.build_batch ~num_neighbours:5 ~max_num_neighbours:5
      ~num_neighbours_build:100 data.train in
  let t1 = Unix.gettimeofday () in
  printf "index construction: %f s\n%!" (t1-.t0);
  let num_neighbours = Bigarray.Array2.dim1 data.test_distances in
  (* XXX setting num neighbours search to num neighbours for now,
     since we are missing things in the search algo *)
  let got_distances = Hnsw.Ba.knn_batch hgraph data.test num_neighbours ~num_neighbours_search:(num_neighbours) in
  (* let _ = *)
  (*   fold_cols data.test *)
  (*     ~init:1 *)
  (*     ~f:(fun j test_col -> *)
  (*         let neighbours = *)
  (*           Hnsw.knn_bigarray hgraph test_col num_neighbours ~num_additional_neighbours_search:5 *)
  (*         in *)
  (*         List.iteri neighbours ~f:(fun i e -> *)
  (*             got_distances.{i+1,j} <- e.distance_to_target *)
  (*           ); *)
  (*         j+1 *)
  (*       ) *)
  (* in *)
  let t2 = Unix.gettimeofday () in
  let num_queries = Bigarray.Array2.dim2 data.test |> Float.of_int in
  printf "query: %f s, %f q/s, %f s/q\n%!"
    (t2-.t1) (num_queries/.(t2-.t1)) ((t2-.t1)/.num_queries);
  let recall = Recall.compute data.test_distances got_distances in
  printf "recall: %f\n" recall;
  ();;

main ();;
