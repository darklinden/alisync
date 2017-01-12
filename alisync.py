#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import oss2
import subprocess
import shutil
import sys
import json
import errno

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

def refresh_file(auth_key, auth_sec, cdn_path, remote_path, work_to_death):
    try:
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
    except:
        if work_to_death:
            print("Unexpected error:", sys.exc_info()[0])
            refresh_file(auth_key, auth_sec, cdn_path, remote_path, work_to_death)
        else:
            raise

def upload_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, exclude_paths, dry_run, work_to_death):

    try:
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

                        key_path = key_path.replace("\\", "/")

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
                            if dry_run:
                                print("will upload: \n\tkey: " + key_path + "\n\tfile: " + file_path + "\n\tmd5: " + md5_str + "")
                            else:
                                print("uploading: \n\tkey: " + key_path + "\n\tfile: " + file_path + "\n\tmd5: " + md5_str + "\n\t...")
                                result = bucket.put_object_from_file(key_path, file_path, headers={'Content-MD5': file_md5_base64(file_path)})
                                print("\tresult: " + str(result.status) + "\n\tresponse: " + str(result.headers) + "\n")
                        else:
                            print("file: " + key_path + " matches md5: " + remote_md5 + ", skip.")
    except:
        if work_to_death:
            print("Unexpected error:", sys.exc_info()[0])
            upload_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, exclude_paths, dry_run,
                               work_to_death)
        else:
            raise

def oss_folder_content(bucket, remote_path):
    ret = []
    p = remote_path
    if not remote_path.endswith("/"):
        p += "/"
    for obj in oss2.ObjectIterator(bucket,delimiter='/', prefix=p):
        if obj.is_prefix():  # 文件夹
            # print('directory: ' + obj.key)
            ret.extend(oss_folder_content(bucket, obj.key))
        else:                # 文件
            # print('file: ' + obj.key)
            ret.append(obj)

    return ret


def copy_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, dry_run, work_to_death):

    try:
        auth = oss2.Auth(auth_key, auth_sec)
        bucket = oss2.Bucket(auth, 'oss.aliyuncs.com', key_bucket)

        print("collecting files under: " + local_path + " ...")
        remote_files = oss_folder_content(bucket, local_path)

        for file_obj in remote_files:

            l = local_path
            if not l.endswith("/"):
                l += "/"

            r = remote_path
            if not r.endswith("/"):
                r += "/"

            relative_path = file_obj.key[len(l):]

            remote_key_path = r + relative_path

            try:
                remote_result = bucket.head_object(remote_key_path)
                remote_headers = remote_result.headers
                remote_md5 = remote_headers.get("ETag", "")
                remote_md5 = str(remote_md5)
                remote_md5 = remote_md5.strip()
                remote_md5 = remote_md5.strip("\"")
            except:
                remote_md5 = ""
                pass

            local_md5 = file_obj.etag

            if len(local_md5) > 0 and remote_md5.lower() != local_md5.lower():
                if dry_run:
                    print("will copy: \n\tkey: " + file_obj.key + "\n\tto: " + remote_key_path + "\n\tmd5: " + local_md5 + "")
                else:
                    print("copying: \n\tkey: " + file_obj.key + "\n\tto: " + remote_key_path + "\n\tmd5: " + local_md5 + "\n\t...")

                    bucket.delete_object(remote_key_path)
                    result = bucket.copy_object(key_bucket, file_obj.key, remote_key_path)

                    print("\tresult: " + str(result.status) + "\n\tresponse: " + str(result.headers) + "\n")
            else:
                print("file: " + remote_key_path + " matches md5: " + local_md5 + ", skip.")
    except:
        if work_to_death:
            print("Unexpected error:", sys.exc_info()[0])
            copy_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, dry_run, work_to_death)
        else:
            raise

def mkdir_p(path):
    # print("mkdir_p: " + path)
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def download_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, dry_run, work_to_death):

    try:
        auth = oss2.Auth(auth_key, auth_sec)
        bucket = oss2.Bucket(auth, 'oss.aliyuncs.com', key_bucket)

        print("collecting files under: " + remote_path + " ...")
        remote_files = oss_folder_content(bucket, remote_path)

        for file_obj in remote_files:

            r = remote_path
            if not r.endswith("/"):
                r += "/"

            relative_path = file_obj.key[len(r):]
            if len(relative_path) == 0:
                continue

            l = local_path
            if not l.endswith("/"):
                l += "/"

            local_file_path = l + relative_path

            if os.path.isfile(local_file_path):
                local_md5 = file_md5(local_file_path)
            else:
                local_md5 = ""

            remote_md5 = file_obj.etag

            if len(remote_md5) > 0 and remote_md5.lower() != local_md5.lower():
                if dry_run:
                    print("will download: \n\tkey: " + file_obj.key + "\n\tto: " + local_file_path + "\n\tmd5: " + remote_md5 + "")
                else:
                    print("downloading: \n\tkey: " + file_obj.key + "\n\tto: " + local_file_path + "\n\tmd5: " + remote_md5 + "\n\t...")
                    folder, name = os.path.split(local_file_path)
                    mkdir_p(folder)
                    if os.path.isfile(local_file_path):
                        os.remove(local_file_path)
                    if os.path.isdir(local_file_path):
                        shutil.rmtree(local_file_path)
                    result = bucket.get_object_to_file(file_obj.key, local_file_path)
                    print("\tresult: " + str(result.status) + "\n\tresponse: " + str(result.headers) + "\n")
            else:
                print("file: " + local_file_path + " matches md5: " + remote_md5 + ", skip.")
    except:
        if work_to_death:
            print("Unexpected error:", sys.exc_info()[0])
            download_sync_folder(auth_key, auth_sec, key_bucket, local_path, remote_path, dry_run, work_to_death)
        else:
            raise

def __main__():

    # self_install
    if len(sys.argv) > 1 and sys.argv[1] == 'install':
        self_install("alisync.py", "/usr/local/bin")
        return

    # upload download

    dry_run = False
    action_key = "upload"
    key_bucket = ""
    local_path = ""
    remote_path = ""
    cdn_path = ""
    auth_key = ""
    auth_sec = ""
    work_to_death = False

    exclude_paths = []

    auth_key, auth_sec = read_cfg()

    if len(auth_key) == 0 or len(auth_sec) == 0:
        print("please config key and sec at ~/.alisync")
        return

    auth_key = bytearray(auth_key, 'utf-8')
    auth_sec = bytearray(auth_sec, 'utf-8')

    argLen = len(sys.argv)

    idx = 1
    while idx < argLen:
        cmd_s = sys.argv[idx]
        if cmd_s[0] == "-":
            cmd = cmd_s[1:]
            v = sys.argv[idx + 1]
            if cmd == "dry":
                if v == "1":
                    dry_run = True
                else:
                    dry_run = False
            if cmd == "w2d":
                if v == "1":
                    work_to_death = True
                else:
                    work_to_death = False
            elif cmd == "a":
                action_key = v
            elif cmd == "b":
                key_bucket = v
            elif cmd == "l":
                local_path = v
            elif cmd == "c":
                cdn_path = v
            elif cmd == "r":
                remote_path = v
            elif cmd == "ex":
                idx += 1
                break
            idx += 2
        else:
            idx += 1

    if dry_run:
        print("dry_run: True")
    else:
        print("dry_run: False")

    print("action: " + action_key)
    print("key_bucket: " + key_bucket)
    print("local_path: " + local_path)
    print("cdn_path: " + cdn_path)
    print("remote_path: " + remote_path)

    if len(key_bucket) == 0 or len(local_path) == 0 or len(cdn_path) == 0 or len(remote_path) == 0:
        print("using alisync -a [upload, copy, down] -dry [1 dry-run] -b [bucket-key] -l [local-folder-path] -c [cdn-path] -r [remote-key-path] -ex [exclude-path1] [exclude-path2] ... to sync")
        return

    while idx < argLen:
        tmp = sys.argv[idx]
        tmp = tmp.strip()
        tmp = tmp.strip("'")
        tmp = tmp.strip('"')
        exclude_paths.append(tmp)
        idx += 1

    print("exclude_paths: " + json.dumps(exclude_paths))

    if action_key == "upload":
        # upload sync folder

        upload_sync_folder(auth_key=auth_key, auth_sec=auth_sec, key_bucket=key_bucket, local_path=local_path, remote_path=remote_path, exclude_paths=exclude_paths, dry_run=dry_run, work_to_death=work_to_death)

        # refresh
        if not dry_run:
            refresh_file(auth_key=auth_key, auth_sec=auth_sec, cdn_path=cdn_path, remote_path=remote_path, work_to_death=work_to_death)

    elif action_key == "copy":
        # copy files in buket
        copy_sync_folder(auth_key=auth_key, auth_sec=auth_sec, key_bucket=key_bucket, local_path=local_path, remote_path=remote_path, dry_run=dry_run, work_to_death=work_to_death)

        # refresh
        if not dry_run:
            refresh_file(auth_key=auth_key, auth_sec=auth_sec, cdn_path=cdn_path, remote_path=remote_path, work_to_death=work_to_death)

    elif action_key == "down":
        # download
        download_sync_folder(auth_key=auth_key, auth_sec=auth_sec, key_bucket=key_bucket, local_path=local_path, remote_path=remote_path, dry_run=dry_run, work_to_death=work_to_death)

    print("Done")

__main__()
