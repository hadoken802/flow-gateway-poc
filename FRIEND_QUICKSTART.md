# Friend Quickstart

## 上传多张参考图

```bash
python examples\client_python.py ^
  --image D:\images\img1.jpg ^
  --image D:\images\img2.jpg ^
  --image D:\images\img3.jpg ^
  --prompt "Use all uploaded images as reference inputs. Treat the first as the opening look and the last as the ending look." ^
  --output D:\videos\result.mp4
```

可以只传一张 `--image`，也可以重复传多张。顺序按命令行出现顺序保留。

当前系统不内置首尾帧模式；图片的作用由 `--prompt` 自己描述。
