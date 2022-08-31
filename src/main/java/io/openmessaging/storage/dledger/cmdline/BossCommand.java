/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.cmdline;

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.DLedger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class BossCommand {

    private static Logger logger = LoggerFactory.getLogger(BossCommand.class);

    public static void main(String args[]) {
        Map<String, BaseCommand> commands = new HashMap<>();
        commands.put("server", new ServerCommand());
        commands.put("append", new AppendCommand());
        commands.put("get", new GetCommand());
        commands.put("readFile", new ReadFileCommand());
        commands.put("leadershipTransfer", new LeadershipTransferCommand());
        JCommander.Builder builder = JCommander.newBuilder();
        for (String cmd : commands.keySet()) {
            builder.addCommand(cmd, commands.get(cmd));
        }
        JCommander jc = builder.build();
        jc.parse(args);

        if (jc.getParsedCommand() == null) {
            jc.usage();
        } else if (jc.getParsedCommand().equals("server")) {
            String[] subArgs = parseServerSubArgs(args);
            if (subArgs == null) {
                logger.error("BossCommand: startup with invalid args");
                System.exit(-1);
            }
            DLedger.main(subArgs);
        } else {
            BaseCommand command = commands.get(jc.getParsedCommand());
            if (null != command) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }

    private static String[] parseServerSubArgs(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("-c".equals(args[i]) || "--config".equals(args[i])) {
                if (!args[i + 1].startsWith("-")) {
                    String[] subArgs = new String[2];
                    System.arraycopy(args, i, subArgs, 0, 2);
                    return subArgs;
                }
                return null;
            }
        }
        String[] subArgs = new String[args.length - 1];
        System.arraycopy(args, 1, subArgs, 0, subArgs.length);
        return subArgs;
    }
}
