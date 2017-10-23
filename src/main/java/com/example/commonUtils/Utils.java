package com.example.commonUtils;

import com.amazonaws.services.cloudfront.model.IllegalUpdateException;
import com.amazonaws.util.StringUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by jerry on 2017/10/22.
 */
public class Utils {


    public static final File createFile(File file) {
        if (!file.exists()) {

            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IllegalUpdateException("Could not create file" + file.getPath() + "'");
            }

        }
        return file;
    }

    public static final void createDirectory(String path) {

        File file = new File(path);

        if (!file.exists() && !file.mkdirs()) {
            throw new IllegalStateException("Could not create directory'" + path + "'");
        }


    }


}
