// estado_civil: Estado civil (Casado = 1, Individuo = 2 smh)
// salario_bruto: Salario Bruto
// dependientes: Menores dependientes

// Referencias:
// https://www.juventudpr.org/idj/childtaxcredit
// https://app.calconic.com/api/embed/calculator/63cd52b1bd0e34001e063608

function get_credito_estimado(estado_civil, salario_bruto, dependientes) {


    const ingreso_maximo = estado_civil == "casado" ? 400000 : 200000;
    const salario_scaled = salario_bruto * 0.0765;
    const dependientes_scaled = dependientes * 1600;
    const salario_diff_scaled = Math.floor((salario_bruto - ingreso_maximo) / 1000) * 50;

    const min_salario_dependientes = Math.min(salario_scaled, dependientes_scaled);

    // let credito_estimado =
    // (salario_bruto <= ingreso_maximo)
    //     ? min_salario_dependientes
    //     : (min_salario_dependientes - salario_diff_scaled > 0)
    //         ? (salario_bruto <= ingreso_maximo ? min_salario_dependientes : min_salario_dependientes - salario_diff_scaled)
    //         : 0;

    if (salario_bruto <= ingreso_maximo) return min_salario_dependientes; // 99% de los casos el salario bruto es menor de $200k or $400k
    if (min_salario_dependientes <= salario_diff_scaled) return 0; // solo when salario_bruto AT LEAST over $200k (o sea nunca)

    if (salario_bruto <= ingreso_maximo) {
        return min_salario_dependientes // this case is never reached wth
    } else {
        return min_salario_dependientes - salario_diff_scaled // Esta es cuando el salario bruto es mayor de $200k o $400k (rarely)
    }

}