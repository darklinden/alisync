#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import oss2
import subprocess
import shutil
import sys
import json

from aliyunsdkcore import client
from aliyunsdkcdn.request.v20141111 import RefreshObjectCachesRequest

def run_cmd(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if err:
        print(err)
    return out

def self_install(file, des):
    file_path = os.path.realpath(file)

    filename = file_path

    pos = filename.rfind("/")
    if pos:
        filename = filename[pos + 1:]

    pos = filename.find(".")
    if pos:
        filename = filename[:pos]

    to_path = os.path.join(des, filename)

    print("installing [" + file_path + "] \n\tto [" + to_path + "]")
    if os.path.isfile(to_path):
        os.remove(to_path)

    shutil.copy(file_path, to_path)
    run_cmd(['chmod', 'a+x', to_path])

def read_cfg():
    home = os.path.expanduser("~")
    file_path = os.path.join(home, ".alisync")

    f = open(file_path, "rb")
    content = f.read()
    f.close()

    cfg = json.loads(content)
    key = cfg.get("key", "")
    sec = cfg.get("sec", "")
    return key, sec

def file_is_ok(file_path, exclude_paths):
    path_list = file_path.split("/")
    for ex_str in exclude_paths:
        for path_str in path_list:
            path_str = path_str.strip()
            path_str = path_str.strip("'")
            path_str = path_str.strip('"')
            if path_str.lower() == ex_str.lower():
                return False

    filename, file_extension = os.path.splitext(file_path)
    if file_extension.lower() == '.php':
        print("skip php file: " + file_path)
        return False
    else:
        return True

def file_md5_base64(file_path):
    f = open(file_path,"rb")
    content = f.read()
    f.close()
    return oss2.utils.content_md5(content)

def file_md5(file_path):
    f = open(file_path,"rb")
    content = f.read()
    f.close()
    return oss2.utils.md5_string(content)

def refresh_file(auth_key, auth_sec, cdn_path, remote_path):
    Client = client.AcsClient(auth_key, auth_sec, 'cn-hangzhou')

    request = RefreshObjectCachesRequest.RefreshObjectCachesRequest()
    request.set_accept_format('json')

    request.set_ObjectType("Directory")

    refresh_path = cdn_path.strip("/")
    refresh_path += "/"
    refresh_path += remote_path.strip("/")
    refresh_path += "/"
    request.set_ObjectPath(refresh_path)

    result = Client.do_action(request)

    print(result)

def sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, exclude_paths):

    auth = oss2.Auth(auth_key, auth_sec)
    bucket = oss2.Bucket(auth, 'oss.aliyuncs.com', key_bucket)

    if os.path.isdir(local_path):
        for root, dirs, files in os.walk(local_path):
            sub_files = os.listdir(root)
            for fn in sub_files:
                file_path = root + "/" + fn
                if os.path.isfile(file_path):
                    if not file_is_ok(file_path, exclude_paths=exclude_paths):
                        # print('skip ' + file_path)
                        continue

                    if len(root) > len(local_path):
                        key_path = os.path.join(root[len(local_path):], fn)
                    else:
                        key_path = fn

                    if key_path[0] == "/":
                        key_path = remote_path + key_path
                    else:
                        key_path = remote_path + "/" + key_path

                    md5_str = file_md5(file_path)

                    try:
                        remote_result = bucket.head_object(key_path)
                        remote_headers = remote_result.headers
                        remote_md5 = remote_headers.get("ETag", "")
                        remote_md5 = str(remote_md5)
                        remote_md5 = remote_md5.strip()
                        remote_md5 = remote_md5.strip("\"")
                    except:
                        remote_md5 = ""
                        pass

                    # print("file: " + key_path + " remote md5: " + remote_md5 + " md5: " + md5_str)

                    if remote_md5.lower() != md5_str.lower():
                        print("uploading: \n\tkey: " + key_path + "\n\tfile: " + file_path + "\n\tmd5: " + md5_str + "\n\t...")
                        result = bucket.put_object_from_file(key_path, file_path, headers={'Content-MD5': file_md5_base64(file_path)})
                        print("\tresult: " + str(result.status) + "\n\tresponse: " + str(result.headers) + "\n")
                    else:
                        print("file: " + key_path + " matches md5: " + remote_md5 + ", skip.")


def __main__():

    # self_install
    if len(sys.argv) > 1 and sys.argv[1] == 'install':
        self_install("alisync.py", "/usr/local/bin")
        return

    key_bucket = ""
    local_path = ""
    remote_path = ""
    cdn_path = ""
    auth_key = ""
    auth_sec = ""

    exclude_paths = []

    auth_key, auth_sec = read_cfg()

    if len(auth_key) == 0 or len(auth_sec) == 0:
        print("please config key and sec at ~/.alisync")
        return

    auth_key = bytearray(auth_key, 'utf-8')
    auth_sec = bytearray(auth_sec, 'utf-8')

    if len(sys.argv) > 4:
        key_bucket = sys.argv[1]
        local_path = sys.argv[2]
        cdn_path = sys.argv[3]
        remote_path = sys.argv[4]

    if len(key_bucket) == 0 or len(local_path) == 0 or len(cdn_path) == 0 or len(remote_path) == 0:
        print("using alisync [bucket-key] [local-folder-path] [cdn-path] [remote-key-path] -ex [exclude-path1] [exclude-path2] ... to sync")
        return

    if len(sys.argv) > 6:
        if sys.argv[5] == '-ex':
            idx = 6
            while idx < len(sys.argv):
                tmp = sys.argv[idx]
                tmp = tmp.strip()
                tmp = tmp.strip("'")
                tmp = tmp.strip('"')
                exclude_paths.append(tmp)
                idx += 1

    sync_folder(auth_key=auth_key, auth_sec=auth_sec, key_bucket=key_bucket, local_path=local_path, remote_path=remote_path, exclude_paths=exclude_paths)

    refresh_file(auth_key=auth_key, auth_sec=auth_sec, cdn_path=cdn_path, remote_path=remote_path)

    print("Done")

__main__()
