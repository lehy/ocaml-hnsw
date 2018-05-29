open Base
open Stdio

let l2 a b = Hnsw.Ohnsw.distance_l2 a b;;
(* Lacaml.S.Vec.ssqr_diff a b *)

let main () =
  let dim = 784 in
  let n = 1000000 in
  let fill_vec =
    let current = ref 1. in
    fun (a : Lacaml.S.vec) ->
      for i = 1 to dim do
        a.{i} <- !current;
        current := if Float.(!current < 1e4) then !current *. 1.01 else 1.
      done
  in
  let a = Lacaml.S.Vec.make dim 0. in
  let b = Lacaml.S.Vec.make dim 0. in
  fill_vec a;
  let sd = ref 0. in
  let t0 = Unix.gettimeofday () in
  for i = 0 to n - 1 do
    fill_vec b;
    let d = l2 a b in
    sd := !sd +. d
  done;
  let t1 = Unix.gettimeofday () in
  printf "%g\n%!" !sd;
  let dt = t1 -. t0 in
  let nt = (Float.of_int n) /. dt in
  let tn = dt /. (Float.of_int n) in
  printf "%gs/%du %g s/u %g u/s\n%!" dt n tn nt;;
  
main ();;

(*
local variables:
compile-command: "cd .. && jbuilder exec bench_dist/bench_dist.exe"
end:
*)
