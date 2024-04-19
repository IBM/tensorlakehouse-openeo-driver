#!/usr/bin/env python

import certifi
import os


def set_certificate():
    certificate = os.getenv("CERTIFICATE")
    print(certificate)
    path = certifi.where()
    print(f"path={path}")
    with open(path, "w") as f:
        f.write(certificate)
        print(f"New certificate file: {path}")


if __name__ == "__main__":
    set_certificate()
