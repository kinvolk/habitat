import { Component, Input, ViewChild } from "@angular/core";
import { MdTooltip } from "@angular/material";

@Component({
  selector: "hab-copyable",
  template: require("./copyable.component.html")
})
export class CopyableComponent {

  @Input() command: string = "";

  public copied: boolean = false;

  @ViewChild(MdTooltip)
  tooltip: MdTooltip;

  copy(text) {
      let el = document.createElement("input");

      Object.assign(el.style, {
          opacity: "0",
          position: "fixed",
          left: "-200px"
      });

      document.body.appendChild(el);
      el.value = this.command;
      el.select();
      document.execCommand("copy");
      document.body.removeChild(el);

      this.copied = true;
      setTimeout(() => { this.tooltip.show(); }, 10);
      setTimeout(() => { this.copied = false; }, 2500);
  }

  get symbol() {
    return this.copied ? "check" : "copy";
  }

  get title() {
    return this.copied ? "Copied!" : "Copy to clipboard";
  }
}
