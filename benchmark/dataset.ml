open Core_kernel
open Stdio
open Hdf5_caml

let () = Random.init 0;;

module Distance = struct
  type t =
    | Euclidean
    | Unknown of string [@@deriving sexp]
  let of_string = function
    | "euclidean" -> Euclidean
    | x -> Unknown x
end

let brute_force_knn_l2 train test k =
  let num_test = Lacaml.S.Mat.dim2 test in
  let num_train = Lacaml.S.Mat.dim2 train in
  let ret = Lacaml.S.Mat.make k num_test Float.nan in
  for i = 1 to num_test do
    let test_col = Lacaml.S.Mat.col test i in
    let dists = Lacaml.S.Vec.make num_train Float.nan in
    for j = 1 to num_train do
      dists.{j} <- Hnsw.EuclideanBa.distance (Lacaml.S.Mat.col train j) test_col
    done;
    Lacaml.S.Vec.sort dists;
    for j = 1 to k do
      ret.{j,i} <- dists.{j}
    done
  done;
  ret;;

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

  let random ~dim ~num_train ~num_test ~k =
    let train = Lacaml.S.Mat.random dim num_train in
    let test = Lacaml.S.Mat.random dim num_test in
    let ret = {
      train;
      test;
      test_distances = brute_force_knn_l2 train test k (* Lacaml.S.Mat.random k num_test *);
      distance = Distance.Euclidean
    }
    in
    (* Format.printf "distances:\n%a\n%!" Lacaml.S.pp_mat ret.test_distances; *)
    ret

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

  let read ?limit_train ?limit_test f =
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
    let ret = { train; test; test_distances; distance=Distance.of_string distance } in
    let crop mat = function
      | None -> mat
      | Some limit -> Bigarray.Array2.sub_right mat 1 limit
    in
    let ret = { ret with train = crop ret.train limit_train; test = crop ret.test limit_test;
                         test_distances = crop ret.test_distances limit_test } in
    (*   match limit_train with *)
    (*   | None -> ret *)
    (*   | Some limit -> { ret with train = Bigarray.Array2.sub_right ret.train 1 limit } *)
    (* in *)
    ret
  ;;
end;;

module Recall = struct
  (*  XXX check computation  *)
  let compute ?(epsilon=1e-8) expected got =
    (* Caml.Format.printf "recall: expected:@,  @[%a@]@." Lacaml.S.pp_mat expected;
     * Caml.Format.printf "recall: got:@,  @[%a@]@." Lacaml.S.pp_mat got; *)
    let module A = Bigarray.Array2 in
    if A.dim1 expected <> A.dim1 got || A.dim2 expected <> A.dim2 got then
      invalid_arg "Recall.compute: arrrays have unequal shapes";
    let num_queries = A.dim2 expected in
    let num_neighbours = A.dim1 expected in
    let ret = ref 0. in
    for i_query = 1 to num_queries do
      let num_ok = ref 0 in
      for i_neighbour = 1 to num_neighbours do
        (* printf "got: %f expected: %f\n" *)
        (*   got.{i_neighbour, i_query} expected.{num_neighbours, i_query}; *)
        if Float.( <= ) got.{i_neighbour, i_query} (expected.{num_neighbours, i_query} +. epsilon) then
          Int.incr num_ok
      done;
      ret := !ret +. ((Float.of_int !num_ok) /. (Float.of_int num_neighbours))
    done;
    !ret /. (Float.of_int num_queries)
end
