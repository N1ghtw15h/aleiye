package com.aleiye.common.tools;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * @PackageName: com.aleiye.common.tools
 * @ClassName: EncodeTools
 * @Date: 2019/5/22 15:55
 * @Auther: Linxu
 * @Description: TODO
 */
public class EncodeTools {
    /**
     * kylin 用户密码加密
     * @param word
     * @return
     */
    public String getSecurityEncode(String word) {
        PasswordEncoder encoder = new BCryptPasswordEncoder();
        return encoder.encode(word);
    }

    /**
     * kylin 脚本登录时“账号名:密码”加密
     * 调用BASE64Encoder方法
     * @param word
     * @return
     */
    public String getBASE64Encode(String word){
        @SuppressWarnings("restriction")
        String code = new sun.misc.BASE64Encoder() {
            @Override
            protected int bytesPerLine() {
                return 9999;
            }
        }.encode(new String(word).getBytes());
        return code;
    }

    public static void main(String[] args) {
        EncodeTools encodeTools = new EncodeTools();
        System.out.println("123456");
        System.out.println(encodeTools.getSecurityEncode("123456"));
        System.out.println(encodeTools.getBASE64Encode("ADMIN:KYLIN"));
    }
}
