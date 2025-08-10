import {
    FormControl,
    FormField,
    FormItem,
    Input,
    FormLabel,
    FormMessage,
} from "@marzneshin/common/components";
import type { FC, InputHTMLAttributes } from "react";
import { useFormContext } from "react-hook-form";
import { useTranslation } from "react-i18next";

export const TrafficLimitField: FC<InputHTMLAttributes<HTMLElement>> = ({
    disabled,
}) => {
    const form = useFormContext();
    const { t } = useTranslation();

    return (
        <FormField
            control={form.control}
            name="traffic_limit"
            render={({ field }) => (
                <FormItem>
                    <FormLabel>{t("page.admins.traffic-limit")}</FormLabel>
                    <FormControl>
                        <Input type="number" disabled={disabled} {...field} />
                    </FormControl>
                    <FormMessage />
                </FormItem>
            )}
        />
    );
};
