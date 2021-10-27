#! /bin/bash

function make_ca {
    rm -rf root-ca
    mkdir -p root-ca
    cat > root-ca/openssl.conf <<__EOF
[ ca ]
default_ca = ca_default
[ ca_default ]
dir = root-ca
certs = root-ca
new_certs_dir = root-ca
database = root-ca/index.txt
serial = root-ca/serial.txt
RANDFILE = root-ca/rand
certificate = root-ca/root-cert.pem
private_key = root-ca/root-key.pem
default_days = 10000
default_crl_days = 10000
default_md = md5
preserve = no
policy = generic_policy
[ generic_policy ]
countryName = optional
stateOrProvinceName = optional
localityName = optional
organizationName = optional
organizationalUnitName = optional
commonName = supplied
emailAddress = optional
__EOF
    openssl req -x509 -newkey rsa:2048 -nodes -keyout root-key.pem \
            -sha256 -days 10000 -subj "/CN=root.dbmx.net" -out root-cert.pem
    touch root-ca/index.txt
    echo 01 > root-ca/serial.txt
}

function make_agent {
    name="$1"
    openssl genrsa -out "${name}-key-pkcs1.pem" 2048
    openssl pkcs8 -topk8 -nocrypt -in "${name}-key-pkcs1.pem" -out "${name}-key.pem"
    openssl pkey -pubout -in "${name}-key.pem" > "${name}-pubkey.pem"
    openssl req -new -key "${name}-key.pem" -subj "/CN=${name}" -out "${name}-csr.pem"
    openssl ca --batch -in "${name}-csr.pem" -out "${name}-cert.pem" \
            -keyfile root-key.pem -cert root-cert.pem -md sha256 -days 10000 \
            -policy generic_policy -config root-ca/openssl.conf
    rm -f "${name}-key-pkcs1.pem" "${name}-csr.pem"
}

case "$1" in
    dist)
        rm -f *.pem
        make_ca
        make_agent localhost
        make_agent client
        rm -rf root-ca
        ;;
    clean)
        rm -f *.pem
        rm -rf root-ca
        ;;
    ca)
        make_ca
        ;;
    agent)
        if [ -z "$2" ] ; then
            echo "Usage: $0: dist/ca/make [client_cn ...]"
        else
            make_agent "$2"
        fi
        ;;
    *)
        echo "Usage: $0: dist/ca/make [client_cn ...]"
        ;;
esac
