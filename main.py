import logging
import os
import sys
import traceback
import webbrowser

from flask import Flask, jsonify, render_template

from altfe import bridge, handle
from altfe.interface.root import classRoot

rootPath = os.path.split(os.path.realpath(sys.argv[0]))[0] + "/"
rootPathFrozen = sys._MEIPASS + "/" if getattr(sys, "frozen", False) else rootPath

app = Flask(
    __name__,
    template_folder=rootPath + "usr/templates",
    static_folder=rootPath + "usr/static",
)


def load_dotenv_file(path):
    """
    Load .env key=value pairs into process env.
    Existing OS env vars take precedence and will not be overwritten.
    """
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line == "" or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key == "":
                    continue
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                if key not in os.environ:
                    os.environ[key] = value
    except Exception:
        print(traceback.format_exc())


# 路由
@app.route("/")
def home():
    return render_template("%s/index.html" % (SETS["sys"]["theme"]))


@app.route("/<path:path>", methods=["GET", "POST"])
def api(path):
    return jsonify(handle.handleRoute.do(path))


if __name__ == "__main__":
    # Load .env before config initialization. OS env > .env > config.yml > default.
    load_dotenv_file(os.path.join(rootPath, ".env"))

    # Altfe 框架初始化
    classRoot.setENV("rootPath", rootPath)
    classRoot.setENV("rootPathFrozen", rootPathFrozen)
    bridge.bridgeInit().run(hint=True)

    # 加载配置项
    SETS = classRoot.osGet("LIB_INS", "conf").dict("biu_default")

    # 调整日志等级
    if not SETS["sys"]["debug"]:
        cli = sys.modules["flask.cli"]
        cli.show_server_banner = lambda *x: None
        logging.getLogger("werkzeug").setLevel(logging.ERROR)

    # 启动
    try:
        if SETS["sys"]["autoOpen"]:
            webbrowser.open("http://" + SETS["sys"]["host"])
        app.run(
            host=SETS["sys"]["host"].split(":")[0],
            port=SETS["sys"]["host"].split(":")[1],
            debug=SETS["sys"]["debug"],
            threaded=True,
            use_reloader=False,
        )
    except UnicodeDecodeError:
        print("您的计算机名可能存在特殊字符，程序无法正常运行。")
        print(
            "若是 Windows 系统，可以尝试进入「计算机-属性-高级系统设置-计算机名-更改」，修改计算机名，只可含有 ASCII 码支持的字符。"
        )
        input("按任意键退出...")
    except Exception:
        print(traceback.format_exc())
