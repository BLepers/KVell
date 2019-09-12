# KVell: the Design and Implementation of a Fast Persistent Key-Value Store


## Compiling and reproducing results

To run KVell on an Amazon i3.metal instance and reproduce results:

```bash
# Create an Ubuntu Server 18.04 LTS instance on an i3.metal machine and install dependencies
sudo apt install make clang autoconf libtool

# Install gpertools (optionnal, but KVell will be slower without TCMalloc):
cd ~
git clone https://github.com/gperftools/gperftools.git
cd gpertools
./autogen.sh
./configure
make -j 36

# Install KVell
cd ~
git clone https://github.com/BLepers/KVell
cd KVell
make -j
sudo ./scripts/config-aws.sh
./script/run-aws.sh
```

You should get results similar to these stored in `./scripts/sample_results/` (`./scripts/parse.pl ./scripts/sample_results/log*` to see summary).

## Modifying the code and running custom benchmarks

See [OVERVIEW.md](OVERVIEW.md) to understand the logic of the code.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details

## Paper

Baptiste Lepers, Oana Balmau, Karan Gupta, and Willy Zwaenepoel. 2019. KVell: the Design and Implementation of a Fast Persistent Key-Value Store. In Proceedings of SOSP’19: ACM Symposium on Operating Systems Principles (SOSP’19) [[pdf](sosp19-final40.pdf)].
