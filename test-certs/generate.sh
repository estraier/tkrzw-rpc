#! /bin/bash

cat > ca.conf <<__EOF
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

rm -f *.pem

openssl req -x509 -newkey rsa:2048 -nodes -keyout root-key.pem \
  -sha256 -days 10000 -subj "/CN=root.dbmx.net" -out root-cert.pem

function make_cert {
  name="$1"
  rm -rf root-ca
  mkdir -p root-ca
  touch root-ca/index.txt
  echo 01 > root-ca/serial.txt
  openssl genrsa -out "${name}-key.pem" 2048
  openssl req -new -key "${name}-key.pem" -subj "/CN=${name}" -out "${name}-csr.pem"
  openssl ca --batch -in "${name}-csr.pem" -out "${name}-cert.pem" \
    -keyfile root-key.pem -cert root-cert.pem -md sha256 -days 10000 \
    -policy generic_policy -config ca.conf
  rm -rf root-ca
  rm -f "${name}-csr.pem"
}

make_cert localhost
make_cert client

rm ca.conf
