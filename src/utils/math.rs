use polars::prelude::*;

pub fn fibonacci(n: i32, zero: bool, weighted: bool) -> Vec<f64> {
    let n = n.abs().max(2);

    let (mut a, mut b) = if zero {
        (0.0, 1.0)
    } else {
        (1.0, 1.0)
    };

    let mut result = vec![a];
    for _ in 0..n-1 {
        let temp = a;
        a = b;
        b = temp + b;
        result.push(a);
    }

    if weighted {
        let sum: f64 = result.iter().sum();
        if sum > 0.0 {
            for i in 0..result.len() {
                result[i] /= sum;
            }
        }
    }

    result
}


pub fn dot(w: &[f64], x: &[f64]) -> f64 {
    w.iter().zip(x.iter()).map(|(wi, xi)| wi * xi).sum()
}


mod tests {
    use super::*;

    #[test]
    fn test_fibonacci() {
        println!("{:?}", fibonacci(5, false, false));
    }

    #[test]
    fn test_dot() {
        let w = vec![1.0, 2.0, 3.0];
        let x = vec![4.0, 5.0, 6.0];
        assert_eq!(dot(&w, &x), 32.0);
    }
}