# s3obj

a lightweight wrapper to represent S3 objects

```python
from s3obj import S3Object

obj = S3Object.from_s3_path("s3://path/to/file.txt", base_dir="/tmp")

print(obj.exists_remote())

obj.download()

print(obj.local_path)

obj.upload(overwrite=True)
```