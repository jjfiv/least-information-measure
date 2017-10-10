# Text Retrieval based on Least Information Measure
Experiments built in Galago.

## Original Paper

[Weimao Ke. 2017. Text Retrieval based on Least Information Measurement. In Proceedings of the ACM SIGIR International Conference on Theory of Information Retrieval (ICTIR '17). ACM, New York, NY, USA, 125-132. DOI: https://doi.org/10.1145/3121050.3121075](https://doi.org/10.1145/3121050.3121075)

## Cold-start build instructions

    git submodule update --init -- recursive
    cd deps/galago
    ./scripts/installlib.sh
    mvn install -DskipTests
    cd ../..
    mvn install


