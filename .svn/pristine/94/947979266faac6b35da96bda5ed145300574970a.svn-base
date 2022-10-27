package com.eurlanda.datashire.engine.enumeration;

/**
 * Delete：完成抽取一个文件后，删除它
 * Move：移到另外一个文件夹，要求是同一host上，移动时，如果重名，自动改名，比如可以把当前系统时间标记打上去。
 * DoNothing：啥都不做
 * <p/>
 * Created by Juntao.Zhang on 2014/5/23.
 */
public enum PostProcess {
    DELETE(0),/* MOVE(1),*/ DO_NOTHING(1);
    private int value;

    PostProcess(int value) {
        this.value = value;
    }

    public static PostProcess parse(int value) {
        for (PostProcess p : PostProcess.values()) {
            if (p.getValue() - value == 0) {
                return p;
            }
        }
        return null;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
